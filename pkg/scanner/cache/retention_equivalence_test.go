package cache

import (
	"testing"

	treepkg "github.com/bearer/bearer/pkg/scanner/ast/tree"
	"github.com/bearer/bearer/pkg/scanner/detectorset"
	"github.com/bearer/bearer/pkg/scanner/ruleset"
)

// previousCache is the pre-retention-change cache policy. It intentionally
// exists only in this test so output identities can be compared across the
// former and candidate capacities without adding a production setting.
type previousCache struct {
	sharedRules map[int]bool
	shared      map[*treepkg.Node][]entry
	private     map[*treepkg.Node][]entry
}

func newPreviousCache(sharedRules []*ruleset.Rule) *previousCache {
	shared := make(map[int]bool, len(sharedRules))
	for _, rule := range sharedRules {
		shared[rule.Index()] = true
	}
	return &previousCache{
		sharedRules: shared,
		shared:      make(map[*treepkg.Node][]entry),
		private:     make(map[*treepkg.Node][]entry),
	}
}

func (cache *previousCache) dataFor(rule *ruleset.Rule) map[*treepkg.Node][]entry {
	if cache.sharedRules[rule.Index()] {
		return cache.shared
	}
	return cache.private
}

func (cache *previousCache) Get(node *treepkg.Node, rule *ruleset.Rule) (*detectorset.Result, bool) {
	for _, cached := range cache.dataFor(rule)[node] {
		if cached.rule == rule {
			return cached.result, true
		}
	}
	return nil, false
}

func (cache *previousCache) Put(node *treepkg.Node, rule *ruleset.Rule, result *detectorset.Result) {
	data := cache.dataFor(rule)
	if len(data) > previousMaxCacheSize {
		evicted := 0
		for evictedNode := range data {
			if evicted == evictionSize {
				break
			}
			delete(data, evictedNode)
			evicted++
		}
	}
	data[node] = append(data[node], entry{rule: rule, result: result})
}

func (cache *previousCache) clear() {
	clear(cache.private)
}

const previousMaxCacheSize = 1000

type cacheOperation struct {
	node *treepkg.Node
	rule *ruleset.Rule
}

func runCacheOperations(
	cache interface {
		Get(*treepkg.Node, *ruleset.Rule) (*detectorset.Result, bool)
		Put(*treepkg.Node, *ruleset.Rule, *detectorset.Result)
	},
	operations []cacheOperation,
	results map[cacheOperation]*detectorset.Result,
) []*detectorset.Result {
	observed := make([]*detectorset.Result, 0, len(operations))
	for _, operation := range operations {
		result, hit := cache.Get(operation.node, operation.rule)
		if !hit {
			result = results[operation]
			cache.Put(operation.node, operation.rule, result)
		}
		observed = append(observed, result)
	}
	return observed
}

func TestExpandedRetentionPreservesNestedMultiRuleResultIdentities(t *testing.T) {
	sharedRule := ruleset.BuiltinDatatypeRule
	privateRule := ruleset.BuiltinObjectRule
	shared := NewShared([]*ruleset.Rule{sharedRule})
	candidate := enabledCache(shared)
	previous := newPreviousCache([]*ruleset.Rule{sharedRule})

	const nodeCount = previousMaxCacheSize + 125
	nodes := make([]treepkg.Node, nodeCount)
	results := make(map[cacheOperation]*detectorset.Result, nodeCount*2)
	operations := make([]cacheOperation, 0, nodeCount*4)
	for i := range nodes {
		for _, rule := range []*ruleset.Rule{sharedRule, privateRule} {
			operation := cacheOperation{node: &nodes[i], rule: rule}
			results[operation] = &detectorset.Result{Sanitized: i%2 == 0}
		}
	}
	var appendNestedLookups func(int)
	appendNestedLookups = func(index int) {
		operations = append(operations, cacheOperation{node: &nodes[index], rule: sharedRule})
		if index+1 < len(nodes) {
			appendNestedLookups(index + 1)
		}
		operations = append(operations, cacheOperation{node: &nodes[index], rule: privateRule})
	}
	appendNestedLookups(0)
	// Revisit in reverse order to exercise hits and recomputation after the
	// former policy has evicted entries from both shared and private maps.
	for i := len(nodes) - 1; i >= 0; i-- {
		operations = append(operations,
			cacheOperation{node: &nodes[i], rule: sharedRule},
			cacheOperation{node: &nodes[i], rule: privateRule},
		)
	}

	previousResults := runCacheOperations(previous, operations, results)
	candidateResults := runCacheOperations(candidate, operations, results)
	if len(previousResults) != len(candidateResults) {
		t.Fatalf("result count differs: previous=%d candidate=%d", len(previousResults), len(candidateResults))
	}
	for i := range previousResults {
		if previousResults[i] != candidateResults[i] {
			t.Fatalf("result identity differs at operation %d", i)
		}
	}

	var sharedNode *treepkg.Node
	for i := range nodes {
		node := &nodes[i]
		want := results[cacheOperation{node: node, rule: sharedRule}]
		previousResult, previousHit := previous.Get(node, sharedRule)
		candidateResult, candidateHit := candidate.Get(node, sharedRule)
		if previousHit && candidateHit && previousResult == want && candidateResult == want {
			sharedNode = node
			break
		}
	}
	if sharedNode == nil {
		t.Fatal("no shared result remained available in both cache policies")
	}
	previous.clear()
	candidate.Clear()
	if got, hit := previous.Get(sharedNode, sharedRule); !hit || got != results[cacheOperation{node: sharedNode, rule: sharedRule}] {
		t.Fatal("previous cache did not retain a shared result across Clear")
	}
	if got, hit := candidate.Get(sharedNode, sharedRule); !hit || got != results[cacheOperation{node: sharedNode, rule: sharedRule}] {
		t.Fatal("candidate cache did not retain a shared result across Clear")
	}
	if _, hit := previous.Get(sharedNode, privateRule); hit {
		t.Fatal("previous cache retained a private result across Clear")
	}
	if _, hit := candidate.Get(sharedNode, privateRule); hit {
		t.Fatal("candidate cache retained a private result across Clear")
	}
}

func TestExpandedRetentionEvictsExactlyAtFiniteBoundary(t *testing.T) {
	rule := ruleset.BuiltinDatatypeRule
	cache := enabledCache(NewShared(nil))
	nodes := make([]treepkg.Node, maxCacheSize+evictionSize+2)
	for i := 0; i <= maxCacheSize; i++ {
		cache.data[&nodes[i]] = []entry{{rule: rule}}
	}

	insertedNode := &nodes[maxCacheSize+1]
	insertedResult := &detectorset.Result{}
	cache.Put(insertedNode, rule, insertedResult)

	if got, hit := cache.Get(insertedNode, rule); !hit || got != insertedResult {
		t.Fatal("entry inserted after the expanded-boundary eviction was not retained")
	}
	if want := maxCacheSize + 1 - evictionSize + 1; len(cache.data) != want {
		t.Fatalf("cache size after eviction and insertion = %d, want %d", len(cache.data), want)
	}
	remainingOldNodes := 0
	for i := 0; i <= maxCacheSize; i++ {
		if _, exists := cache.data[&nodes[i]]; exists {
			remainingOldNodes++
		}
	}
	if want := maxCacheSize + 1 - evictionSize; remainingOldNodes != want {
		t.Fatalf("retained old node count = %d, want %d", remainingOldNodes, want)
	}
}
