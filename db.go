package bitcask

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

type Bitcask struct {
	indexes map[string]int64
	dbFile  *DBFile
	dirPath string
	mu      sync.RWMutex
}

func Open(dirPath string) (*Bitcask, error) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	dirAbsPath, err := filepath.Abs(dirPath)
	if err != nil {
		return nil, err
	}

	dbFile, err := NewDBFile(dirAbsPath)
	if err != nil {
		return nil, err
	}

	db := &Bitcask{
		dbFile:  dbFile,
		indexes: make(map[string]int64),
		dirPath: dirAbsPath,
	}
	db.loadIndexesFromFile()

	return db, nil
}

func (db *Bitcask) Put(key, value []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	offset := db.dbFile.Offset

	entry := NewEntry(key, value, PUT)

	err = db.dbFile.Write(entry)

	db.indexes[string(key)] = offset

	return

}

func (db *Bitcask) Merge() error {
	if db.dbFile.Offset == 0 {
		return nil
	}

	var (
		validEntries []*Entry
		offset       int64
	)

	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
	}
	mergeDBFile, err := NewMergeDBFile(db.dirPath)

	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(mergeDBFile.File.Name())
	}()

	db.mu.Lock()
	defer db.mu.Unlock()

	for _, entry := range validEntries {
		writeOff := mergeDBFile.Offset
		err = mergeDBFile.Write(entry)
		if err != nil {
			return err
		}

		db.indexes[string(entry.Key)] = writeOff

	}

	// 获取文件名
	dbFileName := db.dbFile.File.Name()
	// 关闭文件
	_ = db.dbFile.File.Close()
	// 删除旧的数据文件
	_ = os.Remove(dbFileName)
	_ = mergeDBFile.File.Close()
	// 获取文件名
	mergeDBFileName := mergeDBFile.File.Name()
	// 临时文件变更为新的数据文件
	_ = os.Rename(mergeDBFileName, filepath.Join(db.dirPath, FileName))

	dbFile, err := NewDBFile(db.dirPath)
	if err != nil {
		return err
	}

	db.dbFile = dbFile
	return nil
}

func (db *Bitcask) exist(key []byte) (int64, error) {
	offset, ok := db.indexes[string(key)]

	if !ok {
		return 0, ErrKeyNotFound
	}

	return offset, nil
}

func (db *Bitcask) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return
	}
	db.mu.RLock()
	defer db.mu.RUnlock()

	offset, err := db.exist(key)
	if err == ErrKeyNotFound {
		return
	}

	var e *Entry
	e, err = db.dbFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}

	if e != nil {
		val = e.Value
	}
	return

}

func (db *Bitcask) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	_, err = db.exist(key)

	if err == ErrKeyNotFound {
		err = nil
		return
	}

	e := NewEntry(key, nil, DEL)
	err = db.dbFile.Write(e)

	if err != nil {
		return
	}

	delete(db.indexes, string(key))
	return

}

func (db *Bitcask) loadIndexesFromFile() {
	if db.dbFile == nil {
		return
	}

	var offset int64

	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}

		db.indexes[string(e.Key)] = offset

		if e.Mark == DEL {
			delete(db.indexes, string(e.Key))
		}

		offset += e.GetSize()
	}

}

func (db *Bitcask) Close() error {
	if db.dbFile == nil {
		return ErrInvalidDBFile
	}

	return db.dbFile.File.Close()
}
