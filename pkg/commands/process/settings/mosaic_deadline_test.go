package settings

import (
	"testing"
	"time"
)

// Mosaic's outer process watchdog remains the normal runtime boundary.
// The discovery/timing amendment preserves independent worker lifecycle and memory protections.
func TestMosaicOverallDeadlineBackstop(t *testing.T) {
	if Timeout != 60*time.Minute {
		t.Fatalf("overall deadline=%s", Timeout)
	}
	if Timeout <= 55*time.Minute {
		t.Fatal("source deadline must exceed maximum outer watchdog")
	}
	durations := map[string]struct{ got, want time.Duration }{
		"file minimum":    {TimeoutFileMinimum, 150 * time.Second},
		"file maximum":    {TimeoutFileMaximum, 810 * time.Second},
		"file grace":      {TimeoutWorkerFileGrace, 5 * time.Second},
		"worker online":   {TimeoutWorkerOnline, 60 * time.Second},
		"worker shutdown": {TimeoutWorkerShutdown, 5 * time.Second},
	}
	for name, v := range durations {
		if v.got != v.want {
			t.Fatalf("%s=%s want %s", name, v.got, v.want)
		}
	}
	if TimeoutFileBytesPerSecond != 250 ||
		MemorySoftMaximum != 1_073_741_824 ||
		MemoryMaximum != 1_342_177_280 ||
		FileSizeMaximum != 32*1024*1024 ||
		FilesPerWorker != 1000 {
		t.Fatal("file/worker limits changed")
	}
	if MemorySoftMaximum >= MemoryMaximum {
		t.Fatal("soft memory threshold must remain below the hard limit")
	}
	const privateRunnerMemoryBytes = uint64(8_000_000_000)
	if 2*MemoryMaximum > privateRunnerMemoryBytes*60/100 {
		t.Fatal("two workers at the configured hard limit exceed 60% of the private runner memory")
	}
}

// This is a compile-time source policy, shared by discovery and fixed loader defaults.
func TestMosaicFixedCompleteInputBoundary(t *testing.T) {
	if FileSizeMaximum != 33554432 {
		t.Fatal("native discovery must equal promised individual-file cap")
	}
}
