package bitcask

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
)

const (
	FileName      = "minibitcask.data"
	MergeFileName = "minibitcask.data.merge"
)

type DBFile struct {
	File          *os.File
	Offset        int64
	HeaderBufPool *sync.Pool
}

func NewDBHelper(filename string) (*DBFile, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	pool := &sync.Pool{New: func() any {
		return make([]byte, entryHeaderSize)
	}}

	return &DBFile{Offset: stat.Size(), File: file, HeaderBufPool: pool}, nil
}

func NewDBFile(path string) (*DBFile, error) {
	fileName := filepath.Join(path, FileName)
	return NewDBHelper(fileName)
}

func NewMergeDBFile(path string) (*DBFile, error) {
	fileName := filepath.Join(path, MergeFileName)
	return NewDBHelper(fileName)
}

func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	buf := df.HeaderBufPool.Get().([]byte)
	defer df.HeaderBufPool.Put(buf)

	if _, err = df.File.ReadAt(buf, offset); err != nil {
		return nil, err
	}

	e = &Entry{
		KeySize:   binary.BigEndian.Uint32(buf[0:4]),
		ValueSize: binary.BigEndian.Uint32(buf[4:8]),
		Mark:      binary.BigEndian.Uint16(buf[8:10]),
	}

	offset += entryHeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return nil, err
		}
		e.Key = key
	}

	offset += int64(e.KeySize)
	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if _, err = df.File.ReadAt(value, offset); err != nil {
			return nil, err
		}
		e.Value = value
	}

	return e, nil
}

func (df *DBFile) Write(e *Entry) (err error) {
	enc, err := e.Encode()
	if err != nil {
		return err
	}

	if _, err = df.File.WriteAt(enc, df.Offset); err != nil {
		return err
	}

	df.Offset += int64(len(enc))
	return nil
}
