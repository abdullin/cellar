package cellar

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
	"log"

	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
)

var compressionLevel = 10

// SetCompressionLevel allows you to set LZ4 compression level used for chunks
func SetCompressionLevel(level int) {
	compressionLevel = level
}

func chainCompressor(w io.Writer) (*lz4.Writer, error) {
	zw := lz4.NewWriter(w)
	zw.Header.CompressionLevel = compressionLevel
	return zw, nil
}

func chainDecompressor(r io.Reader) (io.Reader, error) {
	zr := lz4.NewReader(r)
	return zr, nil
}

func chainDecryptor(key []byte, src io.Reader) (io.Reader, error) {
	var (
		block cipher.Block
		err   error
	)
	if block, err = aes.NewCipher(key); err != nil {
		log.Panic("Failed to create a new cipher from the key")
	}

	iv := make([]byte, aes.BlockSize)

	if _, err = src.Read(iv); err != nil {
		return nil, errors.Wrap(err, "Failed to read IV")
	}

	stream := cipher.NewCFBDecrypter(block, iv)
	reader := &cipher.StreamReader{R: src, S: stream}
	return reader, nil
}

func chainEncryptor(key []byte, w io.Writer) (*cipher.StreamWriter, error) {

	var (
		block cipher.Block
		err   error
	)
	if block, err = aes.NewCipher(key); err != nil {
		log.Panic("Failed to create a new cipher from the key")
	}

	iv := make([]byte, aes.BlockSize)
	if _, err = io.ReadFull(rand.Reader, iv); err != nil {
		panic(err)
	}

	if _, err = w.Write(iv); err != nil {
		return nil, errors.Wrap(err, "Write")
	}
	stream := cipher.NewCFBEncrypter(block, iv)

	writer := &cipher.StreamWriter{S: stream, W: w}
	return writer, nil
}
