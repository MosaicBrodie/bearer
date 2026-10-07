package cache

import (
	"testing"

	treepkg "github.com/bearer/bearer/pkg/scanner/ast/tree"
	"github.com/bearer/bearer/pkg/scanner/detectorset"
	"github.com/bearer/bearer/pkg/scanner/ruleset"
)

func enabledCache(shared *Shared) *Cache {
	return &Cache{
		enabled: true,
		shared:  shared,
		data:    make(map[*treepkg.Node][]entry),
	}
}

func TestPutDoesNotEvictAtThreshold(t *testing.T) {
	rule := ruleset.BuiltinDatatypeRule
	cache := enabledCache(NewShared(nil))
	nodes := make([]treepkg.Node, maxCacheSize+1)
	for i := 0; i < maxCacheSize; i++ {
		cache.data[&nodes[i]] = []entry{{rule: rule}}
	}

	insertedNode := &nodes[maxCacheSize]
	result := &detectorset.Result{}
	cache.Put(insertedNode, rule, result)

	if got, hit := cache.Get(insertedNode, rule); !hit || got != result {
		t.Fatal("insert at the cache threshold was not retained")
	}
	if len(cache.data) != maxCacheSize+1 {
		t.Fatalf("cache size after threshold insert = %d, want %d", len(cache.data), maxCacheSize+1)
	}
}

func TestPutEvictsExactlyEvictionSizeAndKeepsInsertion(t *testing.T) {
	rule := ruleset.BuiltinDatatypeRule
	cache := enabledCache(NewShared(nil))
	nodes := make([]treepkg.Node, maxCacheSize+2)
	for i := 0; i <= maxCacheSize; i++ {
		cache.data[&nodes[i]] = []entry{{rule: rule}}
	}

	insertedNode := &nodes[maxCacheSize+1]
	result := &detectorset.Result{}
	cache.Put(insertedNode, rule, result)

	if got, hit := cache.Get(insertedNode, rule); !hit || got != result {
		t.Fatal("post-eviction insertion was not retained")
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

func TestGetPreservesNilHitsAndFirstDuplicateResult(t *testing.T) {
	rule := ruleset.BuiltinDatatypeRule
	cache := enabledCache(NewShared(nil))
	nilNode := &treepkg.Node{}
	cache.Put(nilNode, rule, nil)
	cache.Put(nilNode, rule, &detectorset.Result{})
	if result, hit := cache.Get(nilNode, rule); !hit || result != nil {
		t.Fatal("nil result was not retained as the first matching cache hit")
	}

	duplicateNode := &treepkg.Node{}
	first := &detectorset.Result{Sanitized: true}
	second := &detectorset.Result{}
	cache.Put(duplicateNode, rule, first)
	cache.Put(duplicateNode, rule, second)
	if result, hit := cache.Get(duplicateNode, rule); !hit || result != first {
		t.Fatal("duplicate entries did not preserve the first matching result")
	}
}

func TestPutSelectsSharedOrPrivateDataAndClearPreservesShared(t *testing.T) {
	rule := ruleset.BuiltinDatatypeRule
	shared := NewShared([]*ruleset.Rule{rule})
	sharedCache := enabledCache(shared)
	sharedNode := &treepkg.Node{}
	sharedResult := &detectorset.Result{}
	sharedCache.Put(sharedNode, rule, sharedResult)
	if got, hit := sharedCache.Get(sharedNode, rule); !hit || got != sharedResult {
		t.Fatal("shared cache entry was not readable")
	}
	if len(sharedCache.data) != 0 || len(shared.data) != 1 {
		t.Fatal("shared rule entry was not stored in the shared map")
	}
	sharedCache.Clear()
	if got, hit := sharedCache.Get(sharedNode, rule); !hit || got != sharedResult {
		t.Fatal("Clear removed a shared cache entry")
	}

	privateCache := enabledCache(NewShared(nil))
	privateNode := &treepkg.Node{}
	privateResult := &detectorset.Result{}
	privateCache.Put(privateNode, rule, privateResult)
	if got, hit := privateCache.Get(privateNode, rule); !hit || got != privateResult {
		t.Fatal("private cache entry was not readable")
	}
	if len(privateCache.data) != 1 || len(privateCache.shared.data) != 0 {
		t.Fatal("rule absent from the shared index was not stored privately")
	}
	privateCache.Clear()
	if _, hit := privateCache.Get(privateNode, rule); hit {
		t.Fatal("Clear retained a private cache entry")
	}
}

func TestDisabledCacheKeepsBuiltinDatatypeException(t *testing.T) {
	shared := NewShared([]*ruleset.Rule{
		ruleset.BuiltinDatatypeRule,
		ruleset.BuiltinObjectRule,
	})
	cache := &Cache{
		shared: shared,
		data:   make(map[*treepkg.Node][]entry),
	}
	node := &treepkg.Node{}
	result := &detectorset.Result{}
	cache.Put(node, ruleset.BuiltinObjectRule, result)
	if _, hit := cache.Get(node, ruleset.BuiltinObjectRule); hit {
		t.Fatal("disabled cache stored a non-datatype builtin result")
	}

	cache.Put(node, ruleset.BuiltinDatatypeRule, result)
	if got, hit := cache.Get(node, ruleset.BuiltinDatatypeRule); !hit || got != result {
		t.Fatal("disabled cache did not preserve the builtin datatype exception")
	}
}
