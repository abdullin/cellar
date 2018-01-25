package cellar

import (
	"crypto/rand"
	fmt "fmt"
	"io"
	"log"
	rnd "math/rand"
	"testing"
	"time"
)

func genRandBytes(size int) []byte {

	key := make([]byte, size)
	var err error
	if _, err = io.ReadFull(rand.Reader, key); err != nil {
		panic(err)
	}
	return key
}

func genSeedBytes(size int, seed int) []byte {
	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = byte((i + seed) % 256)
	}
	return buf
}
func checkSeedBytes(data []byte, seed int) error {
	for i := 0; i < len(data); i++ {
		expect := byte((i + seed) % 256)
		if data[i] != expect {
			return fmt.Errorf("Given seed %d expected %d at position %d but got %d", seed, expect, i, data[i])
		}
	}
	return nil
}

func TestWithClosing(t *testing.T) {

	var w *Writer
	var err error

	folder := getFolder()
	key := genRandBytes(16)
	w, err = NewWriter(folder, 1000, key)

	defer closeWriter(t, w)

	assert(t, err, "NewWriter")

	var valuesWritten int

	var k int

	for j := 0; j < 5; j++ {
		for i := 0; i < 30; i++ {
			valuesWritten += 64

			if _, err = w.Append(genSeedBytes(64, k)); err != nil {
				t.Fatalf("Append failed: %s", err)
			}
			k++

			if k%17 == 0 {
				assertCheckpoint(t, w)
			}
		}

		assertCheckpoint(t, w)
		w.Checkpoint()
		err = w.Close()

		assert(t, err, "Closing")

		w, err = NewWriter(folder, 1000, key)
		assert(t, err, "Opening writer")

	}

	reader := NewReader(folder, key)

	var valuesRead int
	var n int

	err = reader.Scan(func(pos *ReaderInfo, s []byte) error {

		if err := checkSeedBytes(s, n); err != nil {
			t.Fatalf("Failed seed check: %s", err)
		}

		valuesRead += len(s)
		n++

		return nil
	})

	assert(t, err, "ReadAll")

	if valuesRead != valuesWritten {
		t.Fatalf("Expected to read %d bytes but read %d", valuesWritten, valuesRead)
	}
}

func closeWriter(t *testing.T, w *Writer) {
	err := w.Close()
	if err != nil {
		t.Fatalf("Failed to close the writer %s", err)
	}
}

func assertCheckpoint(t *testing.T, w *Writer) {
	_, err := w.Checkpoint()
	if err != nil {
		t.Fatalf("Failed to checkpoint %s", err)
	}
}

func TestUserCheckpoints(t *testing.T) {

	var (
		w   *Writer
		err error
		pos int64
	)

	folder := getFolder()
	key := genRandBytes(16)
	w, err = NewWriter(folder, 1000, key)

	defer closeWriter(t, w)

	assert(t, err, "NewWriter")

	pos, err = w.GetUserCheckpoint("custom")
	assert(t, err, "GetCheckpoint")
	if pos != 0 {
		t.Fatal("Checkpoint should be 0")
	}

	assert(t, w.PutUserCheckpoint("custom", 42), "PutCheckpoint")

	pos, err = w.GetUserCheckpoint("custom")
	assert(t, err, "GetCheckpoint")
	if pos != 42 {
		t.Fatal("Checkpoint should be 42")
	}

}

func TestSingleChunkDB(t *testing.T) {

	log.Print("Starting single chunk")
	defer log.Print("Single chunk over")

	var w *Writer
	var err error

	folder := getFolder()
	key := genRandBytes(16)
	w, err = NewWriter(folder, 1000, key)

	defer closeWriter(t, w)

	assert(t, err, "NewWriter")

	var valuesWritten int
	for i := 0; i < 2; i++ {
		valuesWritten += 64

		if _, err = w.Append(genSeedBytes(64, i)); err != nil {
			t.Fatalf("Append failed: %s", err)
		}
	}
	assertCheckpoint(t, w)
	w.Close()

	var valuesRead int
	var n int

	reader := NewReader(folder, key)

	err = reader.Scan(func(pos *ReaderInfo, s []byte) error {

		if err := checkSeedBytes(s, n); err != nil {
			t.Fatalf("Failed seed check: %s", err)
		}

		valuesRead += len(s)
		n++

		return nil
	})

	assert(t, err, "ReadAll")

	if valuesRead != valuesWritten {
		t.Fatalf("Expected to read %d bytes but read %d", valuesWritten, valuesRead)
	}

}

func TestReadingWithOffset(t *testing.T) {
}

func TestSimpleKey(t *testing.T) {

	var w *Writer
	var err error

	folder := getFolder()
	key := genRandBytes(16)
	w, err = NewWriter(folder, 1000, key)

	defer closeWriter(t, w)

	assert(t, err, "NewWriter")

	var valuesWritten int
	for i := 0; i < 30; i++ {
		valuesWritten += 64

		if _, err = w.Append(genSeedBytes(64, i)); err != nil {
			t.Fatalf("Append failed: %s", err)
		}
	}
	assertCheckpoint(t, w)

	reader := NewReader(folder, key)
	var valuesRead int
	var n int

	err = reader.Scan(func(pos *ReaderInfo, s []byte) error {

		if err := checkSeedBytes(s, n); err != nil {
			t.Fatalf("Failed seed check: %s", err)
		}

		valuesRead += len(s)
		n++

		return nil
	})

	assert(t, err, "ReadAll")

	if valuesRead != valuesWritten {
		t.Fatalf("Expected to read %d bytes but read %d", valuesWritten, valuesRead)
	}
}

type rec struct {
	pos  int64
	seed int
	size int
}

func TestFuzz(t *testing.T) {

	seed := time.Now().UnixNano()
	r := rnd.New(rnd.NewSource(seed))

	folder := getFolder()
	maxIterations := 1000
	maxValueLength := r.Intn(1024*128) + 10
	maxBufferSize := r.Intn(maxValueLength*maxIterations/2) + 1
	key := genRandBytes(16)

	t.Logf("maxVal %d; maxBuffer %d; seed %d", maxValueLength, maxBufferSize, seed)

	var writer *Writer
	var err error

	var recs []rec

	for i := 0; i <= maxIterations; i++ {
		if r.Intn(17) == 13 || i == maxIterations {
			if writer != nil {
				assertCheckpoint(t, writer)
				writer.Checkpoint()
				err = writer.Close()
				assert(t, err, "Closing writer")

				writer = nil
			}

			recordsSaved := len(recs)

			reader := NewReader(folder, key)
			recordPos := 0
			if r.Intn(5) > 2 && recordsSaved > 0 {
				// pick a random pos to scan from
				recordPos = r.Intn(recordsSaved)
			}

			r := recs[recordPos]

			reader.StartPos = r.pos
			scanSeed := r.seed

			var bytesRead int
			var recordsRead int
			var bytesWritten int
			var recordsWritten int
			for i := recordPos; i < recordsSaved; i++ {
				bytesWritten += recs[i].size
				recordsWritten++
			}

			reader.Scan(func(p *ReaderInfo, b []byte) error {
				bytesRead += len(b)
				recordsRead++
				if err := checkSeedBytes(b, scanSeed); err != nil {
					t.Fatalf("Failed to verify data: %s", err)
				}
				scanSeed++
				return nil
			})
			if bytesWritten != bytesRead {
				t.Fatalf("Written %d bytes but read %d bytes from %d (%d). Records: %d, %d", bytesWritten, bytesRead, reader.StartPos, bytesRead+int(reader.StartPos), recordsWritten, recordsRead)
			}
		}

		if writer == nil {
			writer, err = NewWriter(folder, int64(maxBufferSize), key)
			assert(t, err, "new writer")
		}

		valSize := r.Intn(maxValueLength)

		val := genSeedBytes(valSize, i)
		pos := writer.VolatilePos()
		_, err = writer.Append(val)

		recs = append(recs, rec{
			pos:  pos,
			seed: i,
			size: valSize,
		})
		if err != nil {
			assert(t, err, "append")
		}
	}
}
