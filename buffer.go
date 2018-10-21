package cellar

import (
	"bufio"
	"crypto/cipher"
	"io"
	"log"
	"os"
	"path"

	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
)

type Buffer struct {
	fileName string
	maxBytes int64
	startPos int64

	records int64
	pos     int64

	writer *bufio.Writer
	stream *os.File
}

func openBuffer(d *BufferDto, folder string) (*Buffer, error) {

	if len(d.FileName) == 0 {
		return nil, errors.New("empty filename")
	}

	fullPath := path.Join(folder, d.FileName)

	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "Open file")
	}
	f.Truncate(int64(d.MaxBytes))
	if _, err := f.Seek(int64(d.Pos), io.SeekStart); err != nil {
		return nil, errors.Wrap(err, "Seek")
	}

	b := &Buffer{
		fileName: d.FileName,
		startPos: d.StartPos,
		maxBytes: d.MaxBytes,
		pos:      d.Pos,
		records:  d.Records,
		stream:   f,
		writer:   bufio.NewWriter(f),
	}
	return b, nil
}

func (b *Buffer) getState() *BufferDto {
	return &BufferDto{
		FileName: b.fileName,
		MaxBytes: b.maxBytes,
		StartPos: b.startPos,
		Pos:      b.pos,
		Records:  b.records,
	}
}

func (b *Buffer) fits(bytes int64) bool {
	return (b.pos + bytes) <= b.maxBytes
}

func (b *Buffer) writeBytes(bs []byte) error {
	if _, err := b.writer.Write(bs); err != nil {
		return errors.Wrap(err, "Write")
	}
	b.pos += int64(len(bs))
	return nil
}

func (b *Buffer) endRecord() {
	b.records++
}

func (b *Buffer) flush() error {
	if err := b.writer.Flush(); err != nil {
		return errors.Wrap(err, "Flush")
	}
	return nil
}

func (b *Buffer) close() error {
	if b.stream == nil {
		return nil
	}
	var err error
	if err = b.stream.Close(); err != nil {
		return errors.Wrap(err, "stream.Close")
	}
	b.stream = nil
	return nil
}

func (b *Buffer) compress(key []byte) (dto *ChunkDto, err error) {

	loc := b.stream.Name() + ".lz4"

	if err = b.writer.Flush(); err != nil {
		log.Panicf("Failed to flush buffer: %s", err)
	}
	if err = b.stream.Sync(); err != nil {
		log.Panicf("Failed to Fsync buffer: %s", err)
	}

	if _, err = b.stream.Seek(0, io.SeekStart); err != nil {
		log.Panicf("Failed to seek to 0 in buffer: %s", err)
	}

	// create chunk file
	var chunkFile *os.File
	if chunkFile, err = os.Create(loc); err != nil {
		return nil, errors.Wrap(err, "os.Create")
	}

	defer func() {
		if err := chunkFile.Sync(); err != nil {
			panic("Failed to sync")
		}
		if err := chunkFile.Close(); err != nil {
			panic("Failed to close")
		}
	}()

	// buffer writes to file
	buffer := bufio.NewWriter(chunkFile)

	defer buffer.Flush()

	// encrypt before buffering
	var encryptor *cipher.StreamWriter
	if encryptor, err = chainEncryptor(key, buffer); err != nil {
		log.Panicf("Failed to chain encryptor for %s: %s", loc, err)
	}

	defer encryptor.Close()

	// compress before encrypting

	var zw *lz4.Writer
	if zw, err = chainCompressor(encryptor); err != nil {
		log.Panicf("Failed to chain compressor: %s", err)
	}

	// copy chunk to the chain
	if _, err = io.CopyN(zw, b.stream, b.pos); err != nil {
		return nil, errors.Wrap(err, "CopyN")
	}

	zw.Close()
	chunkFile.Sync()
	b.close()

	var size int64
	if size, err = chunkFile.Seek(0, io.SeekEnd); err != nil {
		return nil, errors.Wrap(err, "Seek")
	}

	dto = &ChunkDto{
		FileName:             b.fileName + ".lz4",
		Records:              b.records,
		UncompressedByteSize: b.pos,
		StartPos:             b.startPos,
		CompressedDiskSize:   size,
	}
	return dto, nil
}
