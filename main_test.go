package cellar

import (
	"os"
	"testing"
)

func getFolder() string {
	return NewTempFolder("cellar")
}

func TestMain(m *testing.M) {
	// setup
	retCode := m.Run()
	RemoveTempFolders()
	os.Exit(retCode)
}

func makeSlice(l int) []byte {
	return make([]byte, l)
}
