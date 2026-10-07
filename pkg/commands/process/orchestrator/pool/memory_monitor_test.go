package pool

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	gopsutilprocess "github.com/shirou/gopsutil/v3/process"

	"github.com/bearer/bearer/pkg/commands/process/orchestrator/work"
	"github.com/bearer/bearer/pkg/commands/process/settings"
)

type memoryMonitorSampler func() (*gopsutilprocess.MemoryInfoStat, error)

func (f memoryMonitorSampler) MemoryInfo() (*gopsutilprocess.MemoryInfoStat, error) { return f() }

type memoryMonitorRoundTrip func(*http.Request) (*http.Response, error)

func (f memoryMonitorRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func memoryMonitorTestProcess(factory memorySamplerFactory) *Process {
	ctx, cancel := context.WithCancel(context.Background())
	return &Process{
		context:              ctx,
		cancelContext:        cancel,
		command:              &exec.Cmd{Process: &os.Process{Pid: 123}},
		errorChannel:         make(chan error, 1),
		exitChannel:          make(chan struct{}),
		memoryMonitorDone:    make(chan struct{}),
		memorySamplerFactory: factory,
	}
}

func startMemoryMonitorForTest(process *Process) {
	go func() {
		defer close(process.memoryMonitorDone)
		process.monitorMemory()
	}()
}

func waitMemoryMonitor(t *testing.T, process *Process) {
	t.Helper()
	select {
	case <-process.memoryMonitorDone:
	case <-time.After(2 * time.Second):
		t.Fatal("memory monitor did not stop")
	}
}

func TestMemoryMonitorStartupFailureCancelsWorkerWithSafeError(t *testing.T) {
	process := memoryMonitorTestProcess(func(context.Context, int32) (memorySampler, error) {
		return nil, errors.New("host-specific detail must not escape")
	})
	startMemoryMonitorForTest(process)
	waitMemoryMonitor(t, process)

	if !errors.Is(process.context.Err(), context.Canceled) {
		t.Fatal("monitor startup failure did not cancel the worker")
	}
	if got := process.memoryMonitorError(); got != ErrorMemoryMonitor {
		t.Fatalf("monitor error = %v, want safe sentinel", got)
	}
	select {
	case got := <-process.errorChannel:
		if got != ErrorMemoryMonitor {
			t.Fatalf("reported error = %v, want safe sentinel", got)
		}
	default:
		t.Fatal("monitor failure was not reported")
	}
}

func TestMemoryMonitorSampleFailureFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sample func() (*gopsutilprocess.MemoryInfoStat, error)
	}{
		{name: "error", sample: func() (*gopsutilprocess.MemoryInfoStat, error) { return nil, errors.New("sample detail") }},
		{name: "nil sample", sample: func() (*gopsutilprocess.MemoryInfoStat, error) { return nil, nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			process := memoryMonitorTestProcess(func(context.Context, int32) (memorySampler, error) {
				return memoryMonitorSampler(tc.sample), nil
			})
			startMemoryMonitorForTest(process)
			waitMemoryMonitor(t, process)
			if process.memoryMonitorError() != ErrorMemoryMonitor || process.context.Err() == nil {
				t.Fatal("sample failure did not fail closed")
			}
		})
	}
}

func TestMemoryMonitorCancellationJoinsAndDoesNotReportFailure(t *testing.T) {
	entered := make(chan struct{})
	process := memoryMonitorTestProcess(func(context.Context, int32) (memorySampler, error) {
		close(entered)
		return memoryMonitorSampler(func() (*gopsutilprocess.MemoryInfoStat, error) {
			return &gopsutilprocess.MemoryInfoStat{RSS: 1}, nil
		}), nil
	})
	close(process.exitChannel)
	startMemoryMonitorForTest(process)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("monitor factory was not called")
	}
	closed := make(chan struct{})
	go func() { process.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not join the memory monitor")
	}
	if process.memoryMonitorError() != nil {
		t.Fatalf("normal cancellation set monitor error: %v", process.memoryMonitorError())
	}
}

func TestMemoryMonitorFailureRacingShutdownIsCancellation(t *testing.T) {
	t.Run("factory", func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		process := memoryMonitorTestProcess(func(context.Context, int32) (memorySampler, error) {
			close(entered)
			<-release
			return nil, errors.New("factory completed after shutdown")
		})
		close(process.exitChannel)
		startMemoryMonitorForTest(process)
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("monitor factory was not entered")
		}
		process.cancelContext()
		close(release)
		process.Close()
		if process.memoryMonitorError() != nil {
			t.Fatalf("shutdown race became monitor failure: %v", process.memoryMonitorError())
		}
	})

	t.Run("sample", func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		process := memoryMonitorTestProcess(func(context.Context, int32) (memorySampler, error) {
			return memoryMonitorSampler(func() (*gopsutilprocess.MemoryInfoStat, error) {
				close(entered)
				<-release
				return nil, errors.New("sample completed after shutdown")
			}), nil
		})
		close(process.exitChannel)
		startMemoryMonitorForTest(process)
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("memory sample was not entered")
		}
		process.cancelContext()
		close(release)
		process.Close()
		if process.memoryMonitorError() != nil {
			t.Fatalf("shutdown race became monitor failure: %v", process.memoryMonitorError())
		}
	})
}

func TestMemoryMonitorHardLimitReportsExistingOOMError(t *testing.T) {
	process := memoryMonitorTestProcess(func(context.Context, int32) (memorySampler, error) {
		return memoryMonitorSampler(func() (*gopsutilprocess.MemoryInfoStat, error) {
			return &gopsutilprocess.MemoryInfoStat{RSS: settings.MemoryMaximum + 1}, nil
		}), nil
	})
	startMemoryMonitorForTest(process)
	waitMemoryMonitor(t, process)
	if process.memoryUsage != settings.MemoryMaximum+1 {
		t.Fatalf("recorded RSS = %d", process.memoryUsage)
	}
	select {
	case got := <-process.errorChannel:
		if got != ErrorOutOfMemory {
			t.Fatalf("error = %v, want existing OOM sentinel", got)
		}
	default:
		t.Fatal("hard limit did not report OOM")
	}
	if process.memoryMonitorError() != ErrorOutOfMemory {
		t.Fatalf("hard RSS threshold health = %v, want OOM sentinel", process.memoryMonitorError())
	}
}

func TestMemoryMonitorSoftLimitRequestsMemoryReduction(t *testing.T) {
	reduced := make(chan struct{}, 1)
	process := memoryMonitorTestProcess(func(context.Context, int32) (memorySampler, error) {
		return memoryMonitorSampler(func() (*gopsutilprocess.MemoryInfoStat, error) {
			return &gopsutilprocess.MemoryInfoStat{RSS: settings.MemorySoftMaximum + 1}, nil
		}), nil
	})
	process.baseURL = "http://worker.invalid"
	process.client = &http.Client{Transport: memoryMonitorRoundTrip(func(request *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(request.URL.Path, work.RouteReduceMemory) {
			t.Errorf("unexpected memory monitor request path %q", request.URL.Path)
		}
		reduced <- struct{}{}
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: make(http.Header)}, nil
	})}
	startMemoryMonitorForTest(process)
	select {
	case <-reduced:
	case <-time.After(2 * time.Second):
		process.cancelContext()
		waitMemoryMonitor(t, process)
		t.Fatal("soft threshold did not request memory reduction")
	}
	process.cancelContext()
	waitMemoryMonitor(t, process)
	if process.memoryMonitorError() != nil {
		t.Fatalf("soft threshold caused monitor failure: %v", process.memoryMonitorError())
	}
}

func TestMemoryMonitorCloseCancelsSoftReductionBeforeJoining(t *testing.T) {
	requestEntered := make(chan struct{})
	process := memoryMonitorTestProcess(func(context.Context, int32) (memorySampler, error) {
		return memoryMonitorSampler(func() (*gopsutilprocess.MemoryInfoStat, error) {
			return &gopsutilprocess.MemoryInfoStat{RSS: settings.MemorySoftMaximum + 1}, nil
		}), nil
	})
	close(process.exitChannel)
	process.baseURL = "http://worker.invalid"
	process.client = &http.Client{Transport: memoryMonitorRoundTrip(func(request *http.Request) (*http.Response, error) {
		close(requestEntered)
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}
	startMemoryMonitorForTest(process)
	select {
	case <-requestEntered:
	case <-time.After(2 * time.Second):
		process.cancelContext()
		waitMemoryMonitor(t, process)
		t.Fatal("soft reduction request did not start")
	}
	closed := make(chan struct{})
	go func() { process.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel and join soft reduction request")
	}
}

func TestMemoryMonitorFailureCannotAcceptCompletedScan(t *testing.T) {
	process := syntheticProcess(coverageRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"FileStats":null,"Error":""}`)),
			Header:     make(http.Header),
		}, nil
	}))
	process.memoryMonitorDone = make(chan struct{})
	process.memorySamplerFactory = func(context.Context, int32) (memorySampler, error) {
		return nil, errors.New("synthetic startup failure")
	}
	process.command = &exec.Cmd{Process: &os.Process{Pid: 123}}
	startMemoryMonitorForTest(process)
	waitMemoryMonitor(t, process)

	_, err := process.Scan(processRequest())
	if err != ErrorMemoryMonitor {
		t.Fatalf("Scan error = %v, want monitor sentinel", err)
	}
}

func TestMemoryMonitorSampleFailureAfterResponseRejectsScan(t *testing.T) {
	sampleEntered := make(chan struct{})
	releaseSample := make(chan struct{})
	responseProduced := make(chan struct{})
	process := syntheticProcess(coverageRoundTrip(func(*http.Request) (*http.Response, error) {
		close(responseProduced)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"FileStats":null,"Error":""}`)),
			Header:     make(http.Header),
		}, nil
	}))
	process.memoryMonitorDone = make(chan struct{})
	process.memorySamplerFactory = func(context.Context, int32) (memorySampler, error) {
		return memoryMonitorSampler(func() (*gopsutilprocess.MemoryInfoStat, error) {
			close(sampleEntered)
			<-releaseSample
			return nil, errors.New("sample failed after response")
		}), nil
	}
	process.command = &exec.Cmd{Process: &os.Process{Pid: 123}}
	startMemoryMonitorForTest(process)
	select {
	case <-sampleEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("memory sample did not start")
	}

	result := make(chan error, 1)
	go func() {
		_, err := process.Scan(processRequest())
		result <- err
	}()
	select {
	case <-responseProduced:
	case <-time.After(time.Second):
		close(releaseSample)
		t.Fatal("worker response was not produced")
	}
	close(releaseSample)
	select {
	case err := <-result:
		if err != ErrorMemoryMonitor {
			t.Fatalf("Scan error = %v, want monitor sentinel", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Scan did not complete after failed memory sample")
	}
}
