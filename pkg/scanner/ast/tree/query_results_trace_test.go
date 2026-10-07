package tree

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func withTestLogger(logger zerolog.Logger) func() {
	previous := log.Logger
	log.Logger = logger
	return func() { log.Logger = previous }
}

func TestQueryResultsPreservesMissingAndStoredResults(t *testing.T) {
	storedNode := &Node{}
	node := &Node{
		tree:         &Tree{types: []string{"synthetic"}},
		queryResults: map[int][]QueryResult{7: {QueryResult{"node": storedNode}}},
	}
	defer withTestLogger(zerolog.New(io.Discard).Level(zerolog.ErrorLevel))()

	if results := node.QueryResults(8); results != nil {
		t.Fatalf("missing query results = %#v, want nil", results)
	}

	stored := node.queryResults[7]
	got := node.QueryResults(7)
	if len(got) != 1 {
		t.Fatalf("present query returned %d results, want 1", len(got))
	}
	if &got[0] != &stored[0] {
		t.Fatalf("present query did not return the stored results slice: got %p, want %p", &got[0], &stored[0])
	}
	if got[0]["node"] != storedNode {
		t.Fatal("present query did not preserve the stored map value pointer")
	}

	if results := (&Node{}).QueryResults(7); results != nil {
		t.Fatalf("nil query map results = %#v, want nil", results)
	}
}

func TestQueryResultsMissingQueryAllocatesNothingWhenTraceDisabled(t *testing.T) {
	restore := withTestLogger(zerolog.New(io.Discard).Level(zerolog.ErrorLevel))
	defer restore()

	node := &Node{
		tree:         &Tree{types: []string{"synthetic"}},
		queryResults: map[int][]QueryResult{1: nil, 2: nil, 3: nil},
	}
	allocations := testing.AllocsPerRun(1000, func() {
		if results := node.QueryResults(99); results != nil {
			panic("missing query returned non-nil results")
		}
	})
	if allocations != 0 {
		t.Fatalf("missing query with trace disabled allocated %v times per call, want 0", allocations)
	}
}

func TestQueryResultsMissingQueryLogsWhenTraceEnabled(t *testing.T) {
	var output bytes.Buffer
	restore := withTestLogger(zerolog.New(&output).Level(zerolog.TraceLevel))
	defer restore()

	node := &Node{
		tree:         &Tree{types: []string{"synthetic"}},
		queryResults: map[int][]QueryResult{3: nil},
	}
	if results := node.QueryResults(17); results != nil {
		t.Fatalf("missing query results = %#v, want nil", results)
	}
	message := output.String()
	for _, expected := range []string{"QueryResults: queryID=17", "node=synthetic", "available IDs=[3]"} {
		if !strings.Contains(message, expected) {
			t.Errorf("trace output %q does not contain %q", message, expected)
		}
	}
}
