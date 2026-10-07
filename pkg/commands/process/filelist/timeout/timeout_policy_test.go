package timeout_test

import (
	"io/fs"
	"testing"
	"time"

	"github.com/bearer/bearer/pkg/commands/process/filelist/timeout"
	"github.com/bearer/bearer/pkg/commands/process/settings"
)

type sizedFileInfo int64

func (info sizedFileInfo) Name() string  { return "policy-test" }
func (info sizedFileInfo) Size() int64   { return int64(info) }
func (sizedFileInfo) Mode() fs.FileMode  { return 0 }
func (sizedFileInfo) ModTime() time.Time { return time.Time{} }
func (sizedFileInfo) IsDir() bool        { return false }
func (sizedFileInfo) Sys() interface{}   { return nil }

func TestAssignUsesBoundedSourcePolicy(t *testing.T) {
	config := settings.Config{Worker: settings.WorkerOptions{
		TimeoutFileMinimum:        settings.TimeoutFileMinimum,
		TimeoutFileMaximum:        settings.TimeoutFileMaximum,
		TimeoutFileBytesPerSecond: settings.TimeoutFileBytesPerSecond,
	}}
	tests := []struct {
		name string
		size int64
		want time.Duration
	}{
		{name: "zero-byte lower bound", size: 0, want: 150 * time.Second},
		{name: "small file lower bound", size: 10_000, want: 150 * time.Second},
		{name: "measured small input lower bound", size: 10_908, want: 150 * time.Second},
		{name: "size-derived timeout", size: 100_000, want: 400 * time.Second},
		{name: "measured large input upper bound", size: 395_922, want: 810 * time.Second},
	}

	var previous time.Duration
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := timeout.Assign(sizedFileInfo(test.size), config)
			if got != test.want {
				t.Fatalf("Assign(%d bytes) = %s, want %s", test.size, got, test.want)
			}
			if got < settings.TimeoutFileMinimum || got > settings.TimeoutFileMaximum {
				t.Fatalf("Assign(%d bytes) = %s, outside [%s, %s]", test.size, got, settings.TimeoutFileMinimum, settings.TimeoutFileMaximum)
			}
			if got < previous {
				t.Fatalf("timeout is not monotonic: %s followed by %s", previous, got)
			}
			previous = got
		})
	}
}
