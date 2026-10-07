package artifact

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	inputfile "github.com/bearer/bearer/pkg/util/file"
)

func openArtifactContextFixture(t *testing.T, root, relative string, content []byte) *inputfile.InputScope {
	t.Helper()
	path := root
	if relative != "." {
		path = filepath.Join(root, relative)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("create fixture input directory: %v", err)
		}
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write fixture input %q: %v", relative, err)
	}
	scope, err := inputfile.OpenInputScope(root)
	if err != nil {
		t.Fatalf("open fixture input scope: %v", err)
	}
	return scope
}

// appendArtifactContextFixture appends provenance produced from the same open
// input scope and actual file bytes used by the fixture. It does not synthesize
// receipt fields or bypass the report's context binding validation.
func appendArtifactContextFixture(t *testing.T, reportPath string, scope *inputfile.InputScope, relative string) {
	t.Helper()
	bound, err := scope.Acquire(relative, nil)
	if err != nil {
		t.Fatalf("acquire fixture input %q: %v", relative, err)
	}
	expected := bound.Expectation()
	if err := bound.Close(); err != nil {
		t.Fatalf("close fixture input %q: %v", relative, err)
	}
	receipt, err := scope.ContextReceipt(expected)
	if err != nil {
		t.Fatalf("create fixture context receipt %q: %v", relative, err)
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatalf("marshal fixture context receipt %q: %v", relative, err)
	}
	file, err := os.OpenFile(reportPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open fixture report for context receipt: %v", err)
	}
	if _, err := file.Write(append(raw, '\n')); err != nil {
		_ = file.Close()
		t.Fatalf("append fixture context receipt: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close fixture report: %v", err)
	}
}
