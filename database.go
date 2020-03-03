package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"

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
func (a Action) FetchBuilds(db *sql.DB, s *State) error {
	servers, err := a.GetServers(db)
	if err != nil {
		return fmt.Errorf("get servers: %w", err)
	}
	for _, server := range servers {
		tx, err := db.BeginTx(a.Context, nil)
		if err != nil {
			return err
		}
		stream, err := s.FetchDeployHistory(a.Context, server)
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

// FetchHeaders scans files with the Unchecked status and downloads just the
// headers. A hit adds the file's headers to the database and sets the status to
// Partial. A miss sets the files status to Missing. The rate argument specifies
// how many files are processed before commiting to the database. A value of 0
// or less uses DefaultCommitRate. The recheck argument forces files with
// Missing status to be included.
func (a Action) FetchHeaders(db *sql.DB, s *State, rate int, recheck bool) error {
	return nil
}

func (a Action) FetchFiles(db *sql.DB, s *State) error {
	return nil
}
