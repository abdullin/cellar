package cellar

import (
	"os"
	"testing"

	"bitbucket.org/abdullin/bitgn/tester"
)

func getFolder() string {
	return tester.NewFolder("cellar")
}

func TestMain(m *testing.M) {
	// setup
	retCode := m.Run()
	tester.RemoveFolders()
	os.Exit(retCode)
}

func makeSlice(l int) []byte {
	return make([]byte, l)
}
