package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anaminus/rbxark/fetch"
	"github.com/anaminus/rbxark/filters"
	"github.com/anaminus/rbxark/objects"
	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/robloxapi/rbxdump/histlog"
)

// FileFlags represents the existence of a file, and the presence of file
// information in the database.
type FileFlags uint8

const (
	NotFound    FileFlags = 0b00001 // File was not found at URL.
	Exists      FileFlags = 0b00010 // File exists. Must never be unset.
	HasHeaders  FileFlags = 0b00100 // File has headers in database.
	HasMetadata FileFlags = 0b01000 // File has metadata in database.
	HasContent  FileFlags = 0b10000 // File has content in objects path.

	// File has not yet been checked.
	Unchecked FileFlags = 0b00000

	// File exists, but was not found at URL.
	Missing FileFlags = NotFound | Exists

	// If (files.flags & Failed == Failed), headers.status contains the failed
	// response status.
	Failed FileFlags = NotFound | HasHeaders
)

func (f FileFlags) String() string {
	if f == Unchecked {
		return "Unchecked"
	}
	var s []string
	if f&NotFound != 0 {
		s = append(s, "NotFound")
	}
	if f&Exists != 0 {
		s = append(s, "Exists")
	}
	if f&HasHeaders != 0 {
		s = append(s, "HasHeaders")
	}
	if f&HasMetadata != 0 {
		s = append(s, "HasMetadata")
	}
	if f&HasContent != 0 {
		s = append(s, "HasContent")
	}
	return strings.Join(s, "|")
}

// Progress returns a string representing progress of the data of a file.
// Results have the following meanings:
//
//     Unchecked : File has not been checked.
//     NotFound  : File was not found because it is either hidden or does not exist.
//     Missing   : File was found previously, but was not found on the latest check.
//     Failed    : File was not found for unexpected reason.
//     Partial   : File exists and has headers.
//     NoContent : File exists, has headers and metadata, but content has gone missing.
//     Complete  : File exists and has headers, metadata, and content.
//
// If a file is in an unusual state, such as having metadata but missing
// content, then the result of String is returned instead.
//
// Certain results do not represent all the information of a value. For example,
// Missing does not indicate the presence or absence of headers, metadata, or
// content.
func (f FileFlags) Progress() string {
	switch {
	case f == Unchecked:
		// File has not been checked.
		return "Unchecked"
	case f&Missing == Missing:
		// File exists, but was not found.
		return "Missing"
	case f&Failed == Failed:
		// File failed to download. Response status stored in headers table.
		return "Failed"
	case f&NotFound != 0:
		// File was not found.
		return "NotFound"
	case f == Exists|HasHeaders:
		// File exists and has headers.
		return "Partial"
	case f == Exists|HasHeaders|HasMetadata:
		// File exists, but content has gone missing.
		return "NoContent"
	case f == Exists|HasHeaders|HasMetadata|HasContent:
		// File exists and has all data.
		return "Complete"
	}
	return f.String()
}

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
			flags    INTEGER NOT NULL DEFAULT 0, -- Corresponds to FileFlags.
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

		CREATE INDEX IF NOT EXISTS build_servers_build ON build_servers(build);
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

// GetFilenames returns a list of filenames from a database.
func (a Action) GetFilenames(e Executor) (filenames []string, err error) {
	const query = `SELECT name FROM filenames`
	rows, err := e.QueryContext(a.Context, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}
		filenames = append(filenames, name)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return
}

// FindManifests returns a list of hashes for existing rbxPkgManifest files.
func (a Action) FindManifests(e Executor) (hashes []string, err error) {
	const query = `
		SELECT metadata.md5 FROM files,metadata
		WHERE metadata.file == files.rowid
		AND files.filename == (
			SELECT rowid FROM filenames
			WHERE name == "rbxPkgManifest.txt"
		)
	`
	rows, err := e.QueryContext(a.Context, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var hash string
		if err = rows.Scan(&hash); err != nil {
			return nil, err
		}
		hashes = append(hashes, hash)
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
func (a Action) FetchBuilds(db *sql.DB, f *fetch.Fetcher, file string) error {
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
// names that aren't already present. Files are added with the Unchecked flags.
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

const DefaultBatchSize = 256

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

type reqEntry struct {
	id     int
	flags  int
	server string
	build  string
	file   string
}

// Combination of extra queries to make.
const (
	qHeaders      = 1 << iota // Upsert all headers.
	qHeaderStatus             // Upsert just the status header.
	qMetadata                 // Upsert metadata.
)

type respEntry struct {
	err error

	id      int
	flags   FileFlags
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

func runFetchContentWorker(ctx context.Context, wg *sync.WaitGroup, f *fetch.Fetcher, objpath string, req *reqEntry, entry *respEntry) {
	defer wg.Done()
	*entry = respEntry{}
	object := objects.NewWriter(objpath)
	var hashes *fetch.HashStore
	if objpath != "" {
		hashes = &fetch.HashStore{}
	}
	respStatus, headers, err := f.FetchContent(ctx, buildFileURL(req.server, req.build, req.file), objpath, hashes, object.AsWriter())
	if err != nil {
		*entry = respEntry{err: fmt.Errorf("fetch content: %w", err)}
		return
	}
	entry.id = req.id
	entry.flags = FileFlags(req.flags)
	entry.respStatus = respStatus
	skipped := false
	if 200 <= respStatus && respStatus < 300 {
		entry.flags |= Exists | HasHeaders
		entry.flags &^= NotFound
		entry.qAction |= qHeaders
		if v, err := strconv.ParseInt(headers.Get("content-length"), 10, 64); err == nil {
			entry.contentLength.Valid = true
			entry.contentLength.Int64 = v
		}
		if v, err := time.Parse(time.RFC1123, headers.Get("last-modified")); err == nil {
			entry.lastModified.Valid = true
			entry.lastModified.Int64 = v.Unix()
		}
		if v := headers.Get("content-type"); v != "" {
			entry.contentType.Valid = true
			entry.contentType.String = v
		}
		if v := headers.Get("etag"); v != "" {
			entry.etag.Valid = true
			entry.etag.String = v
		}
		if object != nil {
			var size int64
			var hash string
			if stat := objects.Stat(objpath, objects.HashFromETag(entry.etag.String)); stat != nil {
				// File exists. The object was not written to, so reuse metadata
				// from the file.
				size = stat.Size()
				hash = strings.ToLower(stat.Name())
				object.Remove()
				skipped = true
			} else {
				if entry.contentLength.Valid {
					object.ExpectSize(entry.contentLength.Int64)
				}
				if size, hash, err = object.Close(); err != nil {
					*entry = respEntry{err: fmt.Errorf("close object %s-%s: %w", req.build, req.file, err)}
					return
				}
			}
			entry.flags |= HasMetadata | HasContent
			entry.qAction |= qMetadata
			entry.hash = hash
			entry.size = size
		}
	} else {
		object.Remove()
		entry.flags |= NotFound
		// 403 is expected if the file is not found. Most file combinations will
		// be this, and the status is already indicated by the NotFound flag, so
		// avoid adding to headers table to save space.
		if respStatus != 403 {
			// Log unexpected status in headers for manual review.
			entry.flags |= HasHeaders
			entry.qAction |= qHeaderStatus
		}
	}
	var skip string
	if skipped {
		skip = "S"
	}
	log.Printf("fetch %-9s %32s %1s from %s-%s (%d)", entry.flags.Progress(), entry.hash, skip, req.build, req.file, req.id)
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
// When downloading file content, the only files considers are Unchecked files,
// and files that have neither the NotFound flag nor the HasContent. A hit
// writes the file to objects, adds the file's headers to the database, sets the
// Exists, HasHeaders, HasMetadata, and HasContent flags, and unsets the
// NotFound flag. A miss sets NotFound flag.
//
// When just retrieving headers, only Unchecked files are considered. A hit adds
// the file's headers to the database, sets the Exists and HasHeaders flags, and
// unsets the NotFound flag. A miss sets the NotFound flag.
//
// If recheck is true, then files with the NotFound flag set are also included.
//
// The batchSize argument specifies how many files are processed before
// committing to the database. A value of 0 or less uses DefaultBatchSize.
func (a Action) FetchContent(db *sql.DB, f *fetch.Fetcher, objpath string, q filters.Query, recheck bool, batchSize int, stats Stats) error {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	var query = `
		WITH temp AS (
			SELECT
				files.rowid AS id,
				files.flags AS flags,
				servers.url AS _server,
				builds.hash AS _build,
				filenames.name AS _file
			FROM files, servers, builds, filenames, build_servers
			WHERE files.build == builds.rowid
			AND files.filename == filenames.rowid
			AND files.build == build_servers.build
			AND build_servers.server == servers.rowid
			AND (
				files.flags == 0 -- Select Unchecked files.
				%s
			)
			%s
			LIMIT ?
		) SELECT * FROM temp
		-- Collapse duplicates caused by build being available from multiple
		-- servers. Note: this really slows down the query.
		GROUP BY _build, _file
	`
	var params []interface{}
	var queryFlags string
	if recheck {
		// Include files that were not found.
		queryFlags += ` OR files.flags & (0) != 0` // NotFound
	}
	if objpath != "" {
		if err := isDir(objpath); err != nil {
			return err
		}
		// Include files that were found and do not have content.
		queryFlags += ` OR files.flags & (17) == 0` // !NotFound && !HasContent
	}
	stmt, err := db.Prepare(fmt.Sprintf(query, queryFlags, q.Expr))
	if err != nil {
		return fmt.Errorf("select files: %w", err)
	}
	params = append(params, q.Params...)
	params = append(params, batchSize)

	reqs := make([]reqEntry, 0, batchSize)
	resps := make([]respEntry, 0, batchSize)
	wg := sync.WaitGroup{}
	for {
		// TODO: Retain duplicate hashes; when a server fails, try the next
		// server. Requires maintaining a map of successful hashes for the
		// duration of the transaction. The map only needs to be as large as
		// rate; successful hashes will not be pulled out of the database again.

		rows, err := stmt.QueryContext(a.Context, params...)
		if err != nil {
			return fmt.Errorf("select files: %w", err)
		}
		reqs = reqs[:0]
		for rows.Next() {
			i := len(reqs)
			reqs = append(reqs, reqEntry{})
			err := rows.Scan(
				&reqs[i].id,
				&reqs[i].flags,
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
			go runFetchContentWorker(a.Context, &wg, f, objpath, &reqs[i], &resps[i])
		}
		log.Printf("fetching %d files...", len(reqs))
		wg.Wait()

		// TODO: fetching is suboptimal because all downloads in the current
		// transaction must complete before the next set of transactions can
		// begin. Downloads from subsequent transactions should start while the
		// downloads from the current transaction are still working.
		//
		// SOLUTION: select a larger number of files, but continue to commit
		// them at the usual rate. The GROUP BY clause makes many results slow
		// to retrieve, so that should be resolved first.

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
			query := `UPDATE files SET flags = ? WHERE rowid = ?`
			params := []interface{}{int(entry.flags), entry.id}
			if entry.qAction&qHeaders != 0 {
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
