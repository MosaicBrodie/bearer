package rulescanner

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/bearer/bearer/pkg/classification/schema"
	"github.com/bearer/bearer/pkg/commands/process/settings"
	"github.com/bearer/bearer/pkg/languages/javascript"
	"github.com/bearer/bearer/pkg/scanner/ast"
	"github.com/bearer/bearer/pkg/scanner/ast/query"
	"github.com/bearer/bearer/pkg/scanner/ast/traversalstrategy"
	treepkg "github.com/bearer/bearer/pkg/scanner/ast/tree"
	"github.com/bearer/bearer/pkg/scanner/cache"
	detectortypes "github.com/bearer/bearer/pkg/scanner/detectors/types"
	"github.com/bearer/bearer/pkg/scanner/detectorset"
	"github.com/bearer/bearer/pkg/scanner/language"
	"github.com/bearer/bearer/pkg/scanner/ruleset"
	"github.com/bearer/bearer/pkg/scanner/variableshape"
)

type noBuiltinsLanguage struct {
	language.Language
}

func (language noBuiltinsLanguage) NewBuiltInDetectors(
	*schema.Classifier,
	*query.Set,
) []detectortypes.Detector {
	return nil
}

type countingDetectorSet struct {
	calls  int
	result *detectorset.Result
}

func (set *countingDetectorSet) DetectAt(
	*treepkg.Node,
	*ruleset.Rule,
	detectortypes.Context,
) (*detectorset.Result, error) {
	set.calls++
	return set.result, nil
}

func largeCacheTestTree(t *testing.T, language language.Language) (*treepkg.Tree, []*treepkg.Node) {
	t.Helper()
	var content strings.Builder
	for i := 0; i < 5_000; i++ {
		fmt.Fprintf(&content, "let value%d = 1;\n", i)
	}

	parsed, err := ast.Parse(context.Background(), language, []byte(content.String()))
	if err != nil {
		t.Fatalf("parse cache test tree: %v", err)
	}
	if parsed.NodeCount() <= 20_000 {
		t.Fatalf("cache test tree has %d nodes; want more than cache enablement threshold", parsed.NodeCount())
	}

	nodes := make([]*treepkg.Node, 0, 1_200)
	var collect func(*treepkg.Node)
	collect = func(node *treepkg.Node) {
		if len(nodes) == cap(nodes) {
			return
		}
		nodes = append(nodes, node)
		for _, child := range node.Children() {
			collect(child)
			if len(nodes) == cap(nodes) {
				return
			}
		}
	}
	collect(parsed.RootNode())
	if len(nodes) != cap(nodes) {
		t.Fatalf("collected %d nodes; want %d", len(nodes), cap(nodes))
	}
	return parsed, nodes
}

func TestExpandedRetentionDoesNotCacheCycleErrorsAndCompletedHitsStayHits(t *testing.T) {
	baseLanguage := javascript.Get()
	testLanguage := noBuiltinsLanguage{Language: baseLanguage}
	parsed, nodes := largeCacheTestTree(t, testLanguage)

	settingsRules := map[string]*settings.Rule{
		"cycle-a": {
			Id:              "cycle-a",
			Languages:       []string{baseLanguage.ID()},
			SanitizerRuleID: "cycle-b",
		},
		"cycle-b": {
			Id:              "cycle-b",
			Languages:       []string{baseLanguage.ID()},
			SanitizerRuleID: "cycle-a",
		},
		"pressure": {
			Id:        "pressure",
			Languages: []string{baseLanguage.ID()},
		},
	}
	ruleSet, err := ruleset.New(baseLanguage.ID(), settingsRules)
	if err != nil {
		t.Fatalf("create ruleset: %v", err)
	}
	shapeSet, err := variableshape.NewSet(testLanguage, ruleSet)
	if err != nil {
		t.Fatalf("create variable shapes: %v", err)
	}
	querySet := query.NewSet(baseLanguage.ID(), baseLanguage.SitterLanguage())
	defer querySet.Close()
	realDetectors, err := detectorset.New(schema.New(schema.Config{}), testLanguage, ruleSet, shapeSet, querySet)
	if err != nil {
		t.Fatalf("create detector set: %v", err)
	}
	cycleA, err := ruleSet.RuleByID("cycle-a")
	if err != nil {
		t.Fatal(err)
	}
	pressureRule, err := ruleSet.RuleByID("pressure")
	if err != nil {
		t.Fatal(err)
	}

	cycleCache := cache.NewCache(parsed, cache.NewShared(ruleSet.Rules()))
	cycleScanner := New(
		context.Background(),
		realDetectors,
		"cache-retention-cycle-test.js",
		nil,
		traversalstrategy.NewCache(parsed.NodeCount()),
		cycleCache,
	)
	if _, err := cycleScanner.detectAtNode(cycleA, nodes[0]); err == nil || !strings.Contains(err.Error(), "cycle found during rule evaluation") {
		t.Fatalf("cold recursive sanitizer evaluation error = %v, want cycle error", err)
	}
	if _, hit := cycleCache.Get(nodes[0], cycleA); hit {
		t.Fatal("cycle error was cached for the cold node")
	}

	// Populate more than the former 1000-node capacity with successful,
	// completed results before evaluating a fresh node in the real cycle.
	for i := 2; i < len(nodes); i++ {
		if _, err := cycleScanner.detectAtNode(pressureRule, nodes[i]); err != nil {
			t.Fatalf("cache pressure lookup %d: %v", i, err)
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := cycleScanner.detectAtNode(cycleA, nodes[1]); err == nil || !strings.Contains(err.Error(), "cycle found during rule evaluation") {
			t.Fatalf("cycle after cache pressure attempt %d error = %v, want cycle error", attempt+1, err)
		}
		if _, hit := cycleCache.Get(nodes[1], cycleA); hit {
			t.Fatalf("cycle error was cached after cache pressure (attempt %d)", attempt+1)
		}
	}

	// Exercise the real rulescanner cache-first path with a counting detector
	// stand-in: only a completed successful result is retained, so a second
	// lookup returns that exact object without re-entering the detector set.
	completed := &detectorset.Result{}
	counting := &countingDetectorSet{result: completed}
	completedCache := cache.NewCache(parsed, cache.NewShared(nil))
	completedScanner := New(
		context.Background(),
		counting,
		"cache-retention-hit-test.js",
		nil,
		traversalstrategy.NewCache(parsed.NodeCount()),
		completedCache,
	)
	builtinRule := ruleset.BuiltinDatatypeRule
	first, err := completedScanner.detectAtNode(builtinRule, nodes[0])
	if err != nil {
		t.Fatalf("first successful detection: %v", err)
	}
	second, err := completedScanner.detectAtNode(builtinRule, nodes[0])
	if err != nil {
		t.Fatalf("completed cache lookup: %v", err)
	}
	if first != completed || second != completed || counting.calls != 1 {
		t.Fatalf("completed hit changed result or re-entered detector: first=%p second=%p calls=%d", first, second, counting.calls)
	}
}
