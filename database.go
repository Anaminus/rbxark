package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/robloxapi/rbxdump/histlog"
)

// FileStatus represents the presence of a file in the database.
type FileStatus int8

const (
	StatusNonextant FileStatus = -2 // File confirmed to not exist.
	StatusMissing   FileStatus = -1 // File may not exist.
	StatusUnchecked FileStatus = +0 // File not checked.
	StatusPartial   FileStatus = +1 // File exists, only headers have been retrieved.
	StatusComplete  FileStatus = +2 // File exists, content also retrieved.
)

type Executor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// sanitizeBaseURL ensure that a given URL is a base URL.
func sanitizeBaseURL(u string) string {
	return strings.TrimRight(u, "/")
}

func buildFileURL(server, hash, file string) string {
	if hash == "" {
		return sanitizeBaseURL(server) + "/" + file
	}
	return sanitizeBaseURL(server) + "/" + hash + "-" + file
}

// Action contains methods that apply to Executers or Queryers.
type Action struct {
	Context context.Context
}

// Init ensures that the necessary tables exist in a database.
func (a Action) Init(e Executor) error {
	const query = `
		PRAGMA foreign_keys = ON;

		-- Set of potential file names. Not all combinations of hashes and
		-- filenames exist.
		CREATE TABLE IF NOT EXISTS filenames (
			rowid INTEGER PRIMARY KEY,
			name  TEXT    NOT NULL UNIQUE -- Name of the file.
		);

		-- Set of URLs representing deployment servers.
		CREATE TABLE IF NOT EXISTS servers (
			rowid INTEGER PRIMARY KEY,
			url   TEXT    NOT NULL UNIQUE -- Base URL from which data is retrieved.
		);

		-- Set of builds retrieved from deployment servers.
		CREATE TABLE IF NOT EXISTS builds (
			rowid   INTEGER PRIMARY KEY,
			hash    TEXT    NOT NULL UNIQUE, -- e.g. "version-0123456789abcdef".
			type    TEXT    NOT NULL,        -- e.g. "WindowsPlayer".
			time    INTEGER NOT NULL,        -- When the build was created.
			version TEXT    NOT NULL         -- e.g. "0.123.1.123456".
		);

		-- Which builds are reported as present on which servers.
		CREATE TABLE IF NOT EXISTS build_servers (
			rowid  INTEGER PRIMARY KEY,
			server INTEGER NOT NULL REFERENCES servers(rowid) ON DELETE CASCADE,
			build  INTEGER NOT NULL REFERENCES builds(rowid) ON DELETE CASCADE,
			UNIQUE (server, build)
		);

		-- Set of actual files.
		CREATE TABLE IF NOT EXISTS files (
			rowid    INTEGER PRIMARY KEY,
			build    INTEGER NOT NULL REFERENCES builds(rowid) ON DELETE CASCADE,
			filename INTEGER NOT NULL REFERENCES filenames(rowid) ON DELETE CASCADE,
			status   INTEGER NOT NULL DEFAULT 0, -- Corresponds to FileStatus.
			UNIQUE (build, filename)
		);

		-- Set of file headers retrieved from deployment server.
		CREATE TABLE IF NOT EXISTS headers (
			rowid          INTEGER PRIMARY KEY,
			file           INTEGER NOT NULL UNIQUE REFERENCES files(rowid) ON DELETE CASCADE,
			status         INTEGER NOT NULL, -- Returned status code.
			content_length INTEGER,          -- Size of the file reported by the server.
			last_modified  INTEGER,          -- Modification time of content on the server.
			content_type   TEXT,             -- Type of file reported by server.
			etag           TEXT              -- MD5 hash (quoted) of the file reported by the server.
		);

		-- Set of attributes associated with each file.
		CREATE TABLE IF NOT EXISTS metadata (
			rowid INTEGER PRIMARY KEY,
			file  INTEGER NOT NULL UNIQUE REFERENCES files(rowid) ON DELETE CASCADE,
			size  INTEGER NOT NULL, -- Size of the file content.
			md5   TEXT NOT NULL     -- MD5 hash of the file content.
		);
	`
	_, err := e.ExecContext(a.Context, query)
	return err
}

// // Migrate migrates old tables to new versions.
// func (db *Database) Migrate()

type Build struct {
	Hash    string
	Type    string
	Time    int64
	Version string
}

// MergeServers updates the list of servers in a database by appending from the
// given list the servers that aren't already in the database.
func (a Action) MergeServers(e Executor, servers []string) (newRows int, err error) {
	if len(servers) == 0 {
		return 0, nil
	}
	query := `INSERT OR IGNORE INTO servers(url) VALUES ` + strings.Repeat(`(?),`, len(servers))
	query = strings.TrimSuffix(query, `,`)
	args := make([]interface{}, len(servers))
	for i, v := range servers {
		args[i] = v
	}
	result, err := e.ExecContext(a.Context, query, args...)
	if err != nil {
		return 0, err
	}
	if result != nil {
		rows, _ := result.RowsAffected()
		newRows = int(rows)
	}
	return newRows, err
}

// MergeFiles updates the list of file names in a database by appending from the
// given list the filenames that aren't already in the database.
func (a Action) MergeFiles(e Executor, files []string) (newRows int, err error) {
	if len(files) == 0 {
		return 0, nil
	}
	query := `INSERT OR IGNORE INTO filenames(name) VALUES ` + strings.Repeat(`(?),`, len(files))
	query = strings.TrimSuffix(query, `,`)
	args := make([]interface{}, len(files))
	for i, v := range files {
		args[i] = v
	}
	result, err := e.ExecContext(a.Context, query, args...)
	if err != nil {
		return 0, err
	}
	if result != nil {
		rows, _ := result.RowsAffected()
		newRows = int(rows)
	}
	return newRows, err
}

// GetServers returns a list of servers from a database.
func (a Action) GetServers(e Executor) (servers []string, err error) {
	const query = `SELECT url FROM servers`
	rows, err := e.QueryContext(a.Context, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var server string
		if err = rows.Scan(&server); err != nil {
			return nil, err
		}
		servers = append(servers, server)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return
}

// AddBuild inserts a single build into a database.
func (a Action) AddBuild(e Executor, server string, build Build) error {
	const query = `
		INSERT OR ABORT INTO builds (hash, type, time, version) VALUES (?, ?, ?, ?);
		INSERT OR ABORT INTO build_servers (server, build) VALUES ((SELECT rowid FROM servers WHERE url=?), last_insert_rowid());
	`
	_, err := e.ExecContext(a.Context, query,
		build.Hash,
		build.Type,
		build.Time,
		build.Version,
		server,
	)
	return err
}

// FetchBuilds downloads and scans the DeployHistory file from each server in
// a database and inserts any new builds into the database.
func (a Action) FetchBuilds(db *sql.DB, f *Fetcher, file string) error {
	servers, err := a.GetServers(db)
	if err != nil {
		return fmt.Errorf("get servers: %w", err)
	}
	for _, server := range servers {
		tx, err := db.BeginTx(a.Context, nil)
		if err != nil {
			return err
		}
		stream, err := f.FetchDeployHistory(a.Context, buildFileURL(server, "", file))
		if err != nil {
			log.Printf("get deploy history: %s", err)
			continue
		}
		var builds []Build
		for _, token := range stream {
			if job, ok := token.(*histlog.Job); ok {
				builds = append(builds, Build{
					Hash:    job.Hash,
					Type:    job.Build,
					Time:    job.Time.Unix(),
					Version: job.Version.String(),
				})
			}
		}
		sort.Slice(builds, func(i, j int) bool {
			return builds[i].Hash < builds[j].Hash
		})
		j := 0
		for i := 1; i < len(builds); i++ {
			if builds[j] != builds[i] {
				j++
				builds[j] = builds[i]
			}
		}
		builds = builds[:j+1]
		count := 0
		for _, build := range builds {
			if err := a.AddBuild(tx, server, build); err != nil {
				if serr := (sqlite3.Error{}); errors.As(err, &serr) && serr.Code == sqlite3.ErrConstraint {
					// Ignore constraint errors.
					continue
				}
				tx.Rollback()
				return fmt.Errorf("add build %s: %w", build.Hash, err)
			}
			count++
		}
		if err := tx.Commit(); err != nil {
			log.Printf("commit tx: %s", err)
			continue
		}
		log.Printf("add %d new builds from %s", count, server)
	}
	return nil
}

// GenerateFiles inserts into a database combinations of build hashes and file
// names that aren't already present. Files are added with the Unchecked status.
func (a Action) GenerateFiles(e Executor) (newRows int, err error) {
	// Insert into files all combinations of builds and filenames that aren't
	// already in files. Slower: Cut `OR IGNORE` and append `EXCEPT SELECT
	// build, filename FROM files`.
	const query = `
		INSERT OR IGNORE INTO files (build, filename)
		SELECT builds.rowid, filenames.rowid FROM filenames, builds
	`
	result, err := e.ExecContext(a.Context, query)
	if err != nil {
		return 0, err
	}
	if result != nil {
		rows, _ := result.RowsAffected()
		newRows = int(rows)
	}
	return newRows, err
}

const DefaultCommitRate = 256

func getHeader(headers http.Header, key string, typ int) interface{} {
	v := headers.Get(key)
	if v == "" {
		return nil
	}
	switch typ {
	case 0:
		return v
	case 1:
		n, err := strconv.ParseInt(v, 10, 63)
		if err != nil {
			return nil
		}
		return n
	case 2:
		t, err := time.Parse(time.RFC1123, v)
		if err != nil {
			return nil
		}
		return t.Unix()
	}
	return nil
}

func isDir(path string) error {
	if stat, err := os.Lstat(path); os.IsNotExist(err) {
		return err
	} else if !stat.IsDir() {
		return fmt.Errorf("%s: not a directory", path)
	}
	return nil
}

func isHex(hash string) bool {
	for _, c := range hash {
		if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
			return false
		}
	}
	return true
}

func objectExists(objects, hash string) bool {
	if len(hash) != 32 || !isHex(hash) {
		return false
	}
	_, err := os.Lstat(filepath.Join(objects, hash[:2], hash))
	return err == nil
}

type ObjectWriter struct {
	objects string
	file    *os.File
	digest  hash.Hash
	size    int64
}

// NewObjectWriter writes an object. If objects is empty, then nil is returned.
// Otherwise, an ObjectWriter is returned, which will write to a temporary file.
// The opening of the file is deferred to the first call to Write.
func NewObjectWriter(objects string) *ObjectWriter {
	if objects == "" {
		return nil
	}
	return &ObjectWriter{
		objects: objects,
		digest:  md5.New(),
	}
}

// AsWriter returns the ObjectWriter as an io.Writer, ensuring that a nil
// pointer results in a nil interface.
func (w *ObjectWriter) AsWriter() io.Writer {
	if w == nil {
		return nil
	}
	return w
}

func (w *ObjectWriter) Write(b []byte) (n int, err error) {
	if w.file == nil {
		w.file, err = ioutil.TempFile(w.objects, ".unresolved_object_*")
		if err != nil {
			return 0, err
		}
	}
	w.digest.Write(b)
	n, err = w.file.Write(b)
	w.size += int64(n)
	return n, err
}

var ErrUnexpectedHash = errors.New("unexpected hash or size")

// Filename returns the location of the underlying temporary file.
func (w *ObjectWriter) Filename() string {
	if w.file == nil {
		return ""
	}
	return w.file.Name()
}

// Close finishes writing the file. A hash of the written content is computed,
// and always returned. The size of the content is also always returned. If a
// specific hash is expected, and the computed hash does not match the expected
// hash, then ErrUnexpectedHash is returned.
//
// If successfully written, the file is moved to the objects directory with the
// hash as the file name. The file is located under a subdirectory that is named
// after the first two characters of the hash. This subdirectory will be created
// if it does not exist.
//
//     hash: d41d8cd98f00b204e9800998ecf8427e
//     path: objects/d4/d41d8cd98f00b204e9800998ecf8427e
//
// If an error occurred, the temporary file will persist. Its location can be
// retrieved with Filename().
func (w *ObjectWriter) Close() (size int64, hash string, err error) {
	var sum [32]byte
	w.digest.Sum(sum[16:16])
	hex.Encode(sum[:], sum[16:])
	hash = string(sum[:])
	if w.file == nil {
		return w.size, hash, nil
	}
	if err = w.file.Sync(); err != nil {
		w.file.Close()
		return w.size, hash, err
	}
	if err = w.file.Close(); err != nil {
		return w.size, hash, err
	}
	dirpath := filepath.Join(w.objects, hash[:2])
	if _, err = os.Lstat(dirpath); os.IsNotExist(err) {
		if err = os.Mkdir(dirpath, 0755); err != nil {
			return w.size, hash, err
		}
	}
	filename := filepath.Join(dirpath, hash)
	if _, err = os.Lstat(filename); !os.IsNotExist(err) {
		// File already exists.
		os.Remove(w.file.Name())
		return w.size, hash, nil
	}
	if err = os.Rename(w.file.Name(), filename); err != nil {
		return w.size, hash, err
	}
	return w.size, hash, nil
}

type reqEntry struct {
	id     int
	status int
	server string
	build  string
	file   string
}

// Combination of extra queries to make.
const (
	qHeaderFull       = 1 << iota // Upsert all headers.
	qHeaderStatus                 // Upsert status header, set other headers to nil.
	qHeaderJustStatus             // Upsert just the status header.
	qMetadata                     // Upsert metadata.
)

type respEntry struct {
	err error

	id      int
	status  FileStatus
	qAction int

	// headers
	respStatus    int
	contentLength sql.NullInt64
	lastModified  sql.NullInt64
	contentType   sql.NullString
	etag          sql.NullString

	// metadata
	hash string
	size int64
}

func runFetchContentWorker(ctx context.Context, wg *sync.WaitGroup, f *Fetcher, objects string, req *reqEntry, entry *respEntry) {
	defer wg.Done()
	*entry = respEntry{}
	object := NewObjectWriter(objects)
	respStatus, headers, err := f.FetchContent(ctx, buildFileURL(req.server, req.build, req.file), objects, object.AsWriter())
	if err != nil {
		*entry = respEntry{err: fmt.Errorf("fetch content: %w", err)}
		return
	}
	entry.id = req.id
	entry.respStatus = respStatus
	if 200 <= respStatus && respStatus < 300 {
		entry.status = StatusPartial
		entry.qAction |= qHeaderFull
		if v, err := strconv.ParseInt(headers.Get("content-length"), 10, 64); err == nil {
			entry.contentLength.Valid = true
			entry.contentLength.Int64 = v
		}
		if v, err := time.Parse(time.RFC1123, headers.Get("last-modified")); err == nil {
			entry.lastModified.Valid = true
			entry.lastModified.Int64 = v.Unix()
		}
		entry.contentType.Valid = true
		entry.contentType.String = headers.Get("content-type")
		entry.etag.Valid = true
		entry.etag.String = headers.Get("etag")
		if object != nil {
			size, hash, err := object.Close()
			if err != nil {
				*entry = respEntry{err: fmt.Errorf("close object %s-%s: %w", req.build, req.file, err)}
				return
			}
			entry.status = StatusComplete
			entry.qAction |= qMetadata
			entry.hash = hash
			entry.size = size
		}
	} else {
		if respStatus == 403 {
			// 403 is expected if the file does not exist (or is not exposed).
			// Most file combinations will be this, and the status is already
			// indicated by files.status, so avoid adding to headers table to
			// save space.
			entry.status = StatusMissing
			if FileStatus(req.status) > StatusUnchecked {
				// File went missing after being initially found. Update just
				// the status, retaining any headers that might already be
				// present.
				entry.qAction |= qHeaderJustStatus
			}
		} else {
			// Otherwise, log the status code for manual review, and assume the
			// file should be rechecked.
			entry.status = StatusUnchecked
			entry.qAction |= qHeaderStatus
		}
	}
	log.Printf("fetched (%d) %s-%s", req.id, req.build, req.file)
}

type Stats map[int]int

func (stats Stats) String() string {
	list := make([]int, 0, len(stats))
	for s := range stats {
		if s != 0 {
			list = append(list, s)
		}
	}
	sort.Ints(list)
	var b strings.Builder
	for _, s := range list {
		fmt.Fprintf(&b, "status %d returned by %d files\n", s, stats[s])
	}
	return b.String()
}

// FetchContent scans files and downloads their content. If objects is not empty
// then the entire file is downloaded to that directory. Otherwise, just the
// headers are retrieved and stored in the database.
//
// When downloading entire files, only files with the Unchecked or Partial
// status are considered. A hit writes the file to objects, adds the file's
// headers to the database, and sets the status to Complete. A miss sets the
// status to Missing.
//
// When just retrieving headers, only files with the Unchecked status are
// considered. A hit adds the file's headers to the database, and sets the
// status to Partial. A miss sets the status to Missing.
//
// If recheck is true, then files with the Missing status are also included.
//
// The rate argument specifies how many files are processed before commiting to
// the database. A value of 0 or less uses DefaultCommitRate.
func (a Action) FetchContent(db *sql.DB, f *Fetcher, objects string, recheck bool, rate int, stats Stats) error {
	if rate <= 0 {
		rate = DefaultCommitRate
	}
	minstatus := StatusUnchecked
	maxstatus := StatusUnchecked
	if recheck {
		minstatus = StatusMissing
	}
	if objects != "" {
		if err := isDir(objects); err != nil {
			return err
		}
		maxstatus = StatusPartial
	}
	reqs := make([]reqEntry, 0, rate)
	resps := make([]respEntry, 0, rate)
	wg := sync.WaitGroup{}
	for {
		const query = `
			WITH temp AS (
				SELECT files.rowid, servers.url, builds.hash, filenames.name
				FROM files, builds, filenames, build_servers, servers
				WHERE files.status BETWEEN ? AND ?
				AND builds.rowid == files.build
				AND filenames.rowid == files.filename
				AND build_servers.build == files.build
				AND build_servers.server == servers.rowid
				LIMIT ?
			) SELECT * FROM temp
			-- Collapse duplicates caused by build being available from multiple
			-- servers. Note: this really slows down the query.
			GROUP BY hash, name
		`
		// TODO: Retain duplicate hashes; when a server fails, try the next
		// server. Requires maintaining a map of successful hashes for the
		// duration of the transaction. The map only needs to be as large as
		// rate; successful hashes will not be pulled out of the database again.
		rows, err := db.QueryContext(a.Context, query, minstatus, maxstatus, rate)
		if err != nil {
			return fmt.Errorf("select files: %w", err)
		}
		reqs = reqs[:0]
		for rows.Next() {
			i := len(reqs)
			reqs = append(reqs, reqEntry{})
			err := rows.Scan(
				&reqs[i].id,
				&reqs[i].server,
				&reqs[i].build,
				&reqs[i].file,
			)
			if err != nil {
				rows.Close()
				return fmt.Errorf("scan row: %w", err)
			}
		}
		if err = rows.Close(); err != nil {
			return fmt.Errorf("finish rows: %w", err)
		}
		if err = rows.Err(); err != nil {
			return fmt.Errorf("row error: %w", err)
		}
		if len(reqs) == 0 {
			break
		}

		resps = resps[:len(reqs)]
		wg.Add(len(reqs))
		for i := range reqs {
			go runFetchContentWorker(a.Context, &wg, f, objects, &reqs[i], &resps[i])
		}
		log.Printf("fetching %d files...", len(reqs))
		wg.Wait()

		tx, err := db.BeginTx(a.Context, nil)
		if err != nil {
			return fmt.Errorf("begin transaction: %w", err)
		}
		log.Printf("committing %d files...", len(reqs))
		for i, entry := range resps {
			if stats != nil {
				stats[entry.respStatus]++
			}
			if entry.err != nil {
				return entry.err
			}
			query := `UPDATE files SET status = ? WHERE rowid = ?`
			params := []interface{}{int(entry.status), entry.id}
			if entry.qAction&qHeaderFull != 0 {
				query += `;
					INSERT INTO headers(
						file,
						status,
						content_length,
						last_modified,
						content_type,
						etag
					)
					VALUES (?, ?, ?, ?, ?, ?)
					ON CONFLICT (file) DO
					UPDATE SET
						status = ?,
						content_length = ?,
						last_modified = ?,
						content_type = ?,
						etag = ?
				`
				params = append(params,
					entry.id,
					entry.respStatus,
					entry.contentLength,
					entry.lastModified,
					entry.contentType,
					entry.etag,

					entry.respStatus,
					entry.contentLength,
					entry.lastModified,
					entry.contentType,
					entry.etag,
				)

			} else if entry.qAction&qHeaderStatus != 0 {
				query += `;
					INSERT INTO headers(file, status)
					VALUES (?, ?)
					ON CONFLICT (file) DO
					UPDATE SET
						status = ?,
						content_length = ?,
						last_modified = ?,
						content_type = ?,
						etag = ?
				`
				params = append(params,
					entry.id, entry.respStatus,
					entry.respStatus, nil, nil, nil, nil,
				)
			} else if entry.qAction&qHeaderJustStatus != 0 {
				query += `;
					INSERT INTO headers(file, status)
					VALUES (?, ?)
					ON CONFLICT (file) DO
					UPDATE SET status = ?
				`
				params = append(params,
					entry.id, entry.respStatus,
					entry.respStatus,
				)
			}
			if entry.qAction&qMetadata != 0 {
				query += `;
					INSERT INTO metadata(file, size, md5)
					VALUES (?, ?, ?)
					ON CONFLICT (file) DO
					UPDATE SET size = ?, md5 = ?
				`
				params = append(params,
					entry.id, entry.size, entry.hash,
					entry.size, entry.hash,
				)
			}
			if _, err = tx.ExecContext(a.Context, query, params...); err != nil {
				tx.Rollback()
				return fmt.Errorf("update file %s-%s: %w", reqs[i].build, reqs[i].file, err)
			}
		}
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}
		log.Printf("committed %d files", len(reqs))
	}
	return nil
}
