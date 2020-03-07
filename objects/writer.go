package objects

import (
	"crypto/md5"
	"encoding/hex"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

// Writer writes an object.
type Writer struct {
	objpath string
	file    *os.File
	digest  hash.Hash
	size    int64
}

// NewWriter returns a new Writer. If objpath is empty, then nil is returned.
// Otherwise, an ObjectWriter is returned, which will write to a temporary file.
// The opening of the file is deferred to the first call to Write.
func NewWriter(objpath string) *Writer {
	if objpath == "" {
		return nil
	}
	return &Writer{
		objpath: objpath,
		digest:  md5.New(),
	}
}

// AsWriter returns the ObjectWriter as an io.Writer, ensuring that a nil
// pointer results in a nil interface.
func (w *Writer) AsWriter() io.Writer {
	if w == nil {
		return nil
	}
	return w
}

// Write implements the io.Writer interface. The first call to Write will
// attempt to open a temporary file, which will then be written to until the
// writer is closed.
func (w *Writer) Write(b []byte) (n int, err error) {
	if w.file == nil {
		w.file, err = ioutil.TempFile(w.objpath, ".unresolved_object_*")
		if err != nil {
			return 0, err
		}
	}
	w.digest.Write(b)
	n, err = w.file.Write(b)
	w.size += int64(n)
	return n, err
}

// Filename returns the location of the underlying temporary file.
func (w *Writer) Filename() string {
	if w.file == nil {
		return ""
	}
	return w.file.Name()
}

// Close finishes writing the file. A hash of the written content is computed,
// and always returned. The size of the content is also always returned.
//
// If successfully written, the file is moved to the objpath directory with the
// hash as the file name. The file is located under a subdirectory that is named
// after the first two characters of the hash. This subdirectory will be created
// if it does not exist.
//
//     hash: d41d8cd98f00b204e9800998ecf8427e
//     path: objects/d4/d41d8cd98f00b204e9800998ecf8427e
//
// If an error occurs, the temporary file will persist. Its location can be
// retrieved with Filename().
func (w *Writer) Close() (size int64, hash string, err error) {
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
	dirpath := filepath.Join(w.objpath, hash[:2])
	if _, err = os.Lstat(dirpath); os.IsNotExist(err) {
		if err = os.Mkdir(dirpath, 0755); err != nil {
			return w.size, hash, err
		}
	}
	filename := filepath.Join(dirpath, hash)
	if _, err = os.Lstat(filename); err == nil {
		// File already exists.
		os.Remove(w.file.Name())
		return w.size, hash, nil
	}
	if err = os.Rename(w.file.Name(), filename); err != nil {
		return w.size, hash, err
	}
	return w.size, hash, nil
}
