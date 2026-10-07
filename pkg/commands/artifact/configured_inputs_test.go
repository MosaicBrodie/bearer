package artifact

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bearer/bearer/pkg/commands/process/filelist/files"
	"github.com/bearer/bearer/pkg/commands/process/settings"
	flagtypes "github.com/bearer/bearer/pkg/flag/types"
	fileutil "github.com/bearer/bearer/pkg/util/file"
	"github.com/stretchr/testify/assert"
)

func TestConfiguredInputsRetainsOrdinaryPromisedFiles(t *testing.T) {
	dir := t.TempDir()
	paths := []string{
		"bundle.min.js",
		"binary.dat",
		"fixtures/archive.tar",
		"font.woff",
		"evidence.md",
	}
	inputs := make([]files.File, 0, len(paths)+1)
	for _, relative := range paths {
		path := filepath.Join(dir, relative)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte{0, 1, 2, 3}, 0o600); err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, files.File{FilePath: relative})
	}
	overLimit := "over-limit.dat"
	overLimitPath := filepath.Join(dir, overLimit)
	if err := os.WriteFile(overLimitPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(overLimitPath, int64(fileutil.MaxPromisedInputFileSize)+1); err != nil {
		t.Fatal(err)
	}
	inputs = append(inputs, files.File{FilePath: overLimit})

	r := &runner{
		targetPath:   dir,
		scanSettings: settings.Config{Scan: flagtypes.ScanOptions{SkipGitIgnore: true}},
	}
	eligible, err := r.configuredInputs(inputs)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(eligible))
	for _, input := range eligible {
		got = append(got, input.FilePath)
	}
	assert.ElementsMatch(t, paths, got)
}
