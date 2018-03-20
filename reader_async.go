package cellar

import (
	"log"
)

type Rec struct {
	Data     []byte
	ChunkPos int64
	StartPos int64
	NextPos  int64
}

func (reader *Reader) ScanAsync(buffer int) chan *Rec {

	vals := make(chan *Rec, buffer)

	go func() {
		// make sure we terminate the channel on scan read
		defer close(vals)

		err := reader.Scan(func(ri *ReaderInfo, data []byte) error {
			vals <- &Rec{data, ri.ChunkPos, ri.StartPos, ri.NextPos}
			return nil
		})

		if err != nil {
			log.Panic(err)
		}
	}()

	return vals
}
