package cellar

import (
	"bytes"
	"encoding/binary"
	"log"

	"github.com/abdullin/lex-go/tuple"
	"github.com/abdullin/mdb"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/bmatsuo/lmdb-go/lmdbscan"
	proto "github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

const (
	BufferTable    byte = 3
	ChunkTable     byte = 1
	MetaTable      byte = 2
	CellarTable    byte = 4
	UserIndexTable byte = 5
)

func lmdbAddChunk(tx *mdb.Tx, chunkStartPos int64, dto *ChunkDto) error {
	key := mdb.CreateKey(ChunkTable, chunkStartPos)

	if err := tx.PutProto(key, dto); err != nil {
		return errors.Wrap(err, "PutProto")
	}

	log.Printf("Added chunk %s with %d records and %d bytes", dto.FileName, dto.Records, dto.UncompressedByteSize)
	return nil
}

func lmdbListChunks(tx *mdb.Tx) ([]*ChunkDto, error) {

	tpl := mdb.CreateKey(ChunkTable)

	scanner := lmdbscan.New(tx.Tx, tx.DB)

	defer scanner.Close()
	scanner.Set(tpl, nil, lmdb.SetRange)

	var chunks []*ChunkDto

	for scanner.Scan() {
		key := scanner.Key()

		if !bytes.HasPrefix(key, tpl) {
			break
		}

		var chunk = &ChunkDto{}
		val := scanner.Val()
		if err := proto.Unmarshal(val, chunk); err != nil {
			return nil, errors.Wrapf(err, "Unmarshal %x at %x", val, key)
		}

		chunks = append(chunks, chunk)
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "Scanner.Scan")
	}
	return chunks, nil
}

func lmdbPutBuffer(tx *mdb.Tx, dto *BufferDto) error {
	tpl := tuple.Tuple([]tuple.Element{BufferTable})

	key := tpl.Pack()
	var val []byte
	var err error

	if val, err = proto.Marshal(dto); err != nil {
		return errors.Wrap(err, "Marshal")
	}
	if err = tx.Put(key, val); err != nil {
		return errors.Wrap(err, "tx.Put")
	}
	return nil
}

func lmdbGetBuffer(tx *mdb.Tx) (*BufferDto, error) {

	tpl := tuple.Tuple([]tuple.Element{BufferTable})
	key := tpl.Pack()
	var data []byte
	var err error

	if data, err = tx.Get(key); err != nil {
		return nil, errors.Wrap(err, "tx.Get")
	}
	if data == nil {
		return nil, nil
	}
	dto := &BufferDto{}
	if err = proto.Unmarshal(data, dto); err != nil {
		return nil, errors.Wrap(err, "Unmarshal")
	}
	return dto, nil
}

func lmdbIndexPosition(tx *mdb.Tx, stream string, k uint64, pos int64) error {
	tpl := tuple.Tuple([]tuple.Element{MetaTable, stream, k})
	key := tpl.Pack()
	var err error

	buf := make([]byte, binary.MaxVarintLen64)

	n := binary.PutVarint(buf, pos)
	if err = tx.Put(key, buf[0:n]); err != nil {
		return errors.Wrap(err, "tx.Put")
	}
	return nil
}

func lmdbLookupPosition(tx *mdb.Tx, stream string, k uint64) (int64, error) {

	tpl := tuple.Tuple([]tuple.Element{MetaTable, stream, k})
	key := tpl.Pack()
	var err error

	var val []byte
	if val, err = tx.Get(key); err != nil {
		return 0, errors.Wrap(err, "tx.Get")
	}
	var pos int64

	pos, _ = binary.Varint(val)
	return pos, nil
}

func lmdbSetCellarMeta(tx *mdb.Tx, m *MetaDto) error {
	key := mdb.CreateKey(CellarTable)
	return tx.PutProto(key, m)
}

func lmdbGetCellarMeta(tx *mdb.Tx) (*MetaDto, error) {

	key := mdb.CreateKey(CellarTable)
	dto := &MetaDto{}
	var err error

	if err = tx.ReadProto(key, dto); err != nil {
		return nil, errors.Wrap(err, "ReadProto")
	}
	return dto, nil

}
