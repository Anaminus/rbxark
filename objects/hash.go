package objects

import (
	"os"
	"path/filepath"
	"strings"
)

// IsHash returns whether the given string is a valid hash.
func IsHash(s string) bool {
	if len(s) != 32 {
		return false
	}
	for _, c := range s {
		if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
			return false
		}
	}
	return true
}

// Exists returns whether an object for a given hash exists in an object path.
// The hash must be lower case. Returns false if objpath is empty.
func Exists(objpath, hash string) bool {
	if objpath == "" {
		return false
	}
	if !IsHash(hash) {
		return false
	}
	_, err := os.Lstat(filepath.Join(objpath, hash[:2], hash))
	return err == nil
}

// Stat returns the file info for the object of a given hash. Returns nil if the
// object does not exist or if objpath is empty.
func Stat(objpath, hash string) os.FileInfo {
	if objpath == "" {
		return nil
	}
	if !IsHash(hash) {
		return nil
	}
	if stat, err := os.Lstat(filepath.Join(objpath, hash[:2], hash)); err == nil {
		return stat
	}
	return nil
}

// Path returns the file path for the object of a given hash. Returns an empty
// string if the hash is invalid or if objpath is empty.
func Path(objpath, hash string) string {
	if objpath == "" {
		return ""
	}
	if !IsHash(hash) {
		return ""
	}
	return filepath.Join(objpath, hash[:2], hash)
}

// HashFromETag attempts to convert an ETag to a valid hash. Returns an empty
// string if the hash could not be converted.
func HashFromETag(etag string) string {
	etag = strings.ToLower(etag)
	etag = strings.TrimPrefix(etag, "w/")
	etag = strings.Trim(etag, "\"")
	if i := strings.Index(etag, "-"); i >= 0 {
		etag = etag[:i]
	}
	if !IsHash(etag) {
		return ""
	}
	return etag
}
