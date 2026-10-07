package settings

import (
	"testing"
	"time"
)

// Mosaic's outer process watchdog remains the normal runtime boundary.
// This finite source backstop must not alter worker/file resource policy.
func TestMosaicOverallDeadlineBackstop(t *testing.T) {
	if Timeout != 30*time.Minute {
		t.Fatalf("overall deadline=%s", Timeout)
	}
	if Timeout <= 24*time.Minute {
		t.Fatal("source deadline must exceed maximum outer watchdog")
	}
	durations := map[string]struct{ got, want time.Duration }{
		"file minimum":    {TimeoutFileMinimum, 5 * time.Second},
		"file maximum":    {TimeoutFileMaximum, 30 * time.Second},
		"file grace":      {TimeoutWorkerFileGrace, 5 * time.Second},
		"worker online":   {TimeoutWorkerOnline, 60 * time.Second},
		"worker shutdown": {TimeoutWorkerShutdown, 5 * time.Second},
	}
	for name, v := range durations {
		if v.got != v.want {
			t.Fatalf("%s=%s want %s", name, v.got, v.want)
		}
	}
	if TimeoutFileBytesPerSecond != 1000 || MemorySoftMaximum != 650000000 || MemoryMaximum != 800000000 || FileSizeMaximum != 2000000 || FilesPerWorker != 1000 {
		t.Fatal("file/worker limits changed")
	}
}
