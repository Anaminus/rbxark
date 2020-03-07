// The pkgman package parses the rbxPkgManifest format.
package pkgman

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

type Entry struct {
	Name         string
	Hash         string
	PackedSize   int64
	UnpackedSize int64
}

func Decode(r io.Reader) (entries []Entry, err error) {
	s := bufio.NewScanner(r)
	s.Split(bufio.ScanLines)
	if !s.Scan() {
		return nil, s.Err()
	}

	switch version := s.Text(); version {
	case "v0":
	default:
		return nil, fmt.Errorf("unexpected version %q", version)
	}

	// Parse v0.
	line := 1
	for s.Scan() {
		line++
		entry := Entry{Name: s.Text()}

		line++
		if !s.Scan() {
			return nil, fmt.Errorf("line %d: expected hash", line)
		}
		entry.Hash = s.Text()

		line++
		if !s.Scan() {
			return nil, fmt.Errorf("line %d: expected packed size", line)
		}
		if entry.PackedSize, err = strconv.ParseInt(s.Text(), 10, 64); err != nil {
			return nil, fmt.Errorf("line %d: parse packed size: %w", line, err)
		}

		line++
		if !s.Scan() {
			return nil, fmt.Errorf("line %d: expected unpacked size", line)
		}
		if entry.UnpackedSize, err = strconv.ParseInt(s.Text(), 10, 64); err != nil {
			return nil, fmt.Errorf("line %d: parse unpacked size: %w", line, err)
		}
		entries = append(entries, entry)
	}
	if err = s.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}
