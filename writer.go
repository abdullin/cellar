package cellar

import (
	"encoding/binary"
	fmt "fmt"
	"log"
	"os"
	"path"

	"github.com/abdullin/mdb"
	"github.com/pkg/errors"
)

type Writer struct {
	db            *mdb.DB
	b             *Buffer
	maxKeySize    int64
	maxValSize    int64
	folder        string
	maxBufferSize int64
	key           []byte
	encodingBuf   []byte
}

func NewWriter(folder string, maxBufferSize int64, key []byte) (*Writer, error) {
	ensureFolder(folder)

	var db *mdb.DB
	var err error

	cfg := mdb.NewConfig()
	// make sure we are writing sync
	cfg.EnvFlags = 0

	if db, err = mdb.New(folder, cfg); err != nil {
		return nil, errors.Wrap(err, "mdb.New")
	}

	var meta *MetaDto
	var b *Buffer

	err = db.Update(func(tx *mdb.Tx) error {
		var err error

		var dto *BufferDto
		if dto, err = lmdbGetBuffer(tx); err != nil {
			return errors.Wrap(err, "lmdbGetBuffer")
		}

		if dto == nil {
			if b, err = createBuffer(tx, 0, maxBufferSize, folder); err != nil {
				return errors.Wrap(err, "SetNewBuffer")
			}
			return nil

		} else if b, err = openBuffer(dto, folder); err != nil {
			return errors.Wrap(err, "openBuffer")
		}

		if meta, err = lmdbGetCellarMeta(tx); err != nil {
			return errors.Wrap(err, "lmdbGetCellarMeta")
		}
		return nil
	})

	if err != nil {
		return nil, errors.Wrap(err, "Update")
	}

	wr := &Writer{
		folder:        folder,
		maxBufferSize: maxBufferSize,
		key:           key,
		encodingBuf:   make([]byte, binary.MaxVarintLen64),
		db:            db,
		b:             b,
	}

	if meta != nil {
		wr.maxKeySize = meta.MaxKeySize
		wr.maxValSize = meta.MaxValSize
	}

	return wr, nil

}

func (w *Writer) VolatilePos() int64 {
	if w.b != nil {
		return w.b.startPos + w.b.pos
	}
	return 0
}

func (w *Writer) Append(data []byte) (pos int64, err error) {

	dataLen := int64(len(data))
	n := binary.PutVarint(w.encodingBuf, dataLen)

	totalSize := n + len(data)

	if !w.b.fits(int64(totalSize)) {
		if err = w.SealTheBuffer(); err != nil {
			return 0, errors.Wrap(err, "SealTheBuffer")
		}
	}

	if err = w.b.writeBytes(w.encodingBuf[0:n]); err != nil {
		return 0, errors.Wrap(err, "write len prefix")
	}
	if err = w.b.writeBytes(data); err != nil {
		return 0, errors.Wrap(err, "write body")
	}

	w.b.endRecord()

	// update statistics
	if dataLen > w.maxValSize {
		w.maxValSize = dataLen
	}

	pos = w.b.startPos + w.b.pos

	return pos, nil
}

func createBuffer(tx *mdb.Tx, startPos int64, maxSize int64, folder string) (*Buffer, error) {
	name := fmt.Sprintf("%012d", startPos)
	dto := &BufferDto{
		Pos:      0,
		StartPos: startPos,
		MaxBytes: maxSize,
		Records:  0,
		FileName: name,
	}
	var err error
	var buf *Buffer

	if buf, err = openBuffer(dto, folder); err != nil {
		return nil, errors.Wrapf(err, "openBuffer %s", folder)
	}

	if err = lmdbPutBuffer(tx, dto); err != nil {
		return nil, errors.Wrap(err, "lmdbPutBuffer")
	}
	return buf, nil

}

func (w *Writer) SealTheBuffer() error {

	var err error

	oldBuffer := w.b
	var newBuffer *Buffer

	if err = oldBuffer.flush(); err != nil {
		return errors.Wrap(err, "buffer.Flush")
	}

	var dto *ChunkDto

	if dto, err = oldBuffer.compress(w.key); err != nil {
		return errors.Wrap(err, "compress")
	}

	newStartPos := dto.StartPos + dto.UncompressedByteSize

	err = w.db.Update(func(tx *mdb.Tx) error {

		if err = lmdbAddChunk(tx, dto.StartPos, dto); err != nil {
			return errors.Wrap(err, "lmdbAddChunk")
		}

		if newBuffer, err = createBuffer(tx, newStartPos, w.maxBufferSize, w.folder); err != nil {
			return errors.Wrap(err, "createBuffer")
		}
		return nil

	})

	if err != nil {
		return errors.Wrap(err, "w.db.Update")
	}

	w.b = newBuffer

	oldBufferPath := path.Join(w.folder, oldBuffer.fileName)

	if err = os.Remove(oldBufferPath); err != nil {
		log.Printf("Can't remove old buffer %s: %s", oldBufferPath, err)
	}
	return nil

}

// Close disposes all resources
func (w *Writer) Close() error {

	// TODO: flush, checkpoint and close current buffer
	return w.db.Close()
}

func (w *Writer) Checkpoint() (int64, error) {
	w.b.flush()

	var err error

	dto := w.b.getState()

	current := dto.StartPos + dto.Pos

	err = w.db.Update(func(tx *mdb.Tx) error {
		var err error

		if err = lmdbPutBuffer(tx, dto); err != nil {
			return errors.Wrap(err, "lmdbPutBuffer")
		}

		meta := &MetaDto{
			MaxKeySize: w.maxKeySize,
			MaxValSize: w.maxValSize,
		}

		if err = lmdbSetCellarMeta(tx, meta); err != nil {
			return errors.Wrap(err, "lmdbSetCellarMeta")
		}
		return nil

	})

	if err != nil {
		return 0, errors.Wrap(err, "txn.Update")
	}

	return current, nil

}
