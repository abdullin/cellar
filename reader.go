package cellar

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	"path"

	"github.com/abdullin/mdb"
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
)

type ReadFlag int

const (
	RF_None        ReadFlag = 0
	RF_LoadBuffer  ReadFlag = 1 << 1
	RF_PrintChunks ReadFlag = 1 << 2
)

type Reader struct {
	Folder      string
	Key         []byte
	Flags       ReadFlag
	StartPos    int64
	EndPos      int64
	LimitChunks int
}

func NewReader(folder string, key []byte) *Reader {
	return &Reader{folder, key, RF_LoadBuffer, 0, 0, 0}
}

type ReaderInfo struct {
	// can be used to convert to file name
	ChunkPos int64
	// global start pos
	StartPos int64
	// global read pos
	NextPos int64
}

type ReadOp func(pos *ReaderInfo, data []byte) error

func (r *Reader) ReadDB(op mdb.TxOp) error {
	var db *mdb.DB
	var err error

	cfg := mdb.NewConfig()
	if db, err = mdb.New(r.Folder, cfg); err != nil {
		return errors.Wrap(err, "mdb.New")
	}

	defer db.Close()

	return db.Read(op)
}

func (r *Reader) Scan(op ReadOp) error {

	var db *mdb.DB
	var err error

	cfg := mdb.NewConfig()
	if db, err = mdb.New(r.Folder, cfg); err != nil {
		return errors.Wrap(err, "mdb.New")
	}

	defer db.Close()

	var b *BufferDto
	// var meta *MetaDto
	var chunks []*ChunkDto

	loadBuffer := (r.Flags & RF_LoadBuffer) == RF_LoadBuffer
	printChunks := (r.Flags & RF_PrintChunks) == RF_PrintChunks

	err = db.Read(func(tx *mdb.Tx) error {
		var err error
		if b, err = lmdbGetBuffer(tx); err != nil {
			return errors.Wrap(err, "lmdbGetBuffer")
		}
		if _, err = lmdbGetCellarMeta(tx); err != nil {
			return errors.Wrap(err, "lmdbGetCellarMeta")
		}
		if chunks, err = lmdbListChunks(tx); err != nil {
			return errors.Wrap(err, "lmdbListChunks")
		}
		return nil

	})

	if err != nil {
		return errors.Wrap(err, "db.Read")
	}

	if b == nil && len(chunks) == 0 {
		return nil
	}

	info := &ReaderInfo{}

	log.Printf("Found %d chunks and limit is %d", len(chunks), r.LimitChunks)

	if len(chunks) > 0 {

		if r.LimitChunks > 0 && len(chunks) > r.LimitChunks {
			log.Printf("Truncating input from %d to %d chunks", len(chunks), r.LimitChunks)
			chunks = chunks[:r.LimitChunks]
		}

		for i, c := range chunks {

			endPos := c.StartPos + c.UncompressedByteSize

			if r.StartPos != 0 && endPos < r.StartPos {
				// skip chunk if it ends before range we are interested in
				continue
			}

			if r.EndPos != 0 && c.StartPos > r.EndPos {
				// skip the chunk if it starts after the range we are interested in
				continue
			}

			chunk := make([]byte, c.UncompressedByteSize)
			var file = path.Join(r.Folder, c.FileName)

			if printChunks {
				log.Printf("Loading chunk %d %s with size %d", i, c.FileName, c.UncompressedByteSize)
			}

			if chunk, err = loadChunkIntoBuffer(file, r.Key, c.UncompressedByteSize, chunk); err != nil {
				log.Panicf("Failed to load chunk %s", c.FileName)
			}

			info.ChunkPos = c.StartPos

			chunkPos := 0
			if r.StartPos != 0 && r.StartPos > c.StartPos {
				// reader starts in the middle
				chunkPos = int(r.StartPos - c.StartPos)
			}

			if err = replayChunk(info, chunk, op, chunkPos); err != nil {
				return errors.Wrap(err, "Failed to read chunk")
			}
		}
	}

	if loadBuffer && b != nil && b.Pos > 0 {

		if r.EndPos != 0 && b.StartPos > r.EndPos {
			// if buffer starts after the end of our search interval - skip it
			return nil
		}

		loc := path.Join(r.Folder, b.FileName)

		var f *os.File

		if f, err = os.Open(loc); err != nil {
			log.Panicf("Failed to open buffer file %s", loc)
		}

		curChunk := make([]byte, b.Pos)

		var n int
		if n, err = f.Read(curChunk); err != nil {
			log.Panicf("Failed to read %d bytes from buffer %s", b.Pos, loc)
		}
		if n != int(b.Pos) {
			log.Panic("Failed to read bytes")
		}

		info.ChunkPos = b.StartPos

		chunkPos := 0

		if r.StartPos > b.StartPos {
			chunkPos = int(r.StartPos - b.StartPos)
		}

		if err = replayChunk(info, curChunk, op, chunkPos); err != nil {
			return errors.Wrap(err, "Failed to read chunk")
		}

	}

	return nil

}

func readVarint(b []byte) (val int64, n int) {

	val, n = binary.Varint(b)
	if n <= 0 {
		log.Panicf("Failed to read varint %d", n)
	}

	return

}

func replayChunk(info *ReaderInfo, chunk []byte, op ReadOp, pos int) error {

	max := len(chunk)

	var err error

	// while we are not at the end,
	// read first len
	// then pass the bytes to the op
	for pos < max {

		info.StartPos = int64(pos) + info.ChunkPos

		recordSize, shift := readVarint(chunk[pos:])

		// move position by the header size
		pos += shift

		// get chunk
		record := chunk[pos : pos+int(recordSize)]
		// apply chunk

		pos += int(recordSize)

		info.NextPos = int64(pos) + info.ChunkPos

		if err = op(info, record); err != nil {
			return errors.Wrap(err, "Failed to execute op")
		}
		// shift pos

	}
	return nil

}

func getMaxByteSize(cs []*ChunkDto, b *BufferDto) int64 {

	var bufferSize int64

	for _, c := range cs {
		if c.UncompressedByteSize > bufferSize {
			bufferSize = c.UncompressedByteSize
		}
	}

	if b != nil && b.MaxBytes > bufferSize {
		bufferSize = b.MaxBytes
	}
	return bufferSize
}

func loadChunkIntoBuffer(loc string, key []byte, size int64, b []byte) ([]byte, error) {

	var decryptor io.Reader
	var err error

	var chunkFile *os.File
	if chunkFile, err = os.Open(loc); err != nil {
		log.Panicf("Failed to open chunk %s", loc)
	}

	defer chunkFile.Close()

	if decryptor, err = chainDecryptor(key, chunkFile); err != nil {
		log.Panicf("Failed to chain decryptor for %s: %s", loc, err)
	}

	zr := lz4.NewReader(decryptor)
	zr.Header.HighCompression = true
	var readBytes int
	if readBytes, err = zr.Read(b); err != nil {
		log.Panicf("Failed to read from chunk %s (%d): %s", loc, size, err)
	}

	if int64(readBytes) != size {
		log.Panicf("Read %d bytes but expected %d", readBytes, size)
	}
	return b[0:readBytes], nil

}
