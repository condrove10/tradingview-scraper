package tradingview

import (
	"os"
	"regexp"
	"testing"
)

// TestReadmeExamplesCompile keeps the README's Go code identical to the
// programs in examples/, which `go vet ./...` and `go build ./...` compile.
func TestReadmeExamplesCompile(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	blocks := regexp.MustCompile("(?s)```go\n(.*?)```").FindAllStringSubmatch(string(readme), -1)
	files := []string{"examples/client/main.go", "examples/callback/main.go"}
	if len(blocks) != len(files) {
		t.Fatalf("README has %d Go blocks, want %d (one per file in examples/)", len(blocks), len(files))
	}
	for i, file := range files {
		want, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if blocks[i][1] != string(want) {
			t.Errorf("README Go block %d differs from %s", i+1, file)
		}
	}
}
