// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cache

import (
	"container/list"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/log"
)

type keyType struct {
	dummyString string
	dummyInt    int
}

func TestLRU(t *testing.T) {
	cache := New(&Options{MaxCount: 5}, nil)

	cache.Put("A", "Foo")
	assert.Equal(t, "Foo", cache.Get("A"))
	assert.Nil(t, cache.Get("B"))
	assert.Equal(t, 1, cache.Size())

	cache.Put("B", "Bar")
	cache.Put("C", "Cid")
	cache.Put("D", "Delt")
	assert.Equal(t, 4, cache.Size())

	assert.Equal(t, "Bar", cache.Get("B"))
	assert.Equal(t, "Cid", cache.Get("C"))
	assert.Equal(t, "Delt", cache.Get("D"))

	cache.Put("A", "Foo2")
	assert.Equal(t, "Foo2", cache.Get("A"))

	cache.Put("E", "Epsi")
	assert.Equal(t, "Epsi", cache.Get("E"))
	assert.Equal(t, "Foo2", cache.Get("A"))
	assert.Nil(t, cache.Get("B")) // Oldest, should be evicted

	// Access C, D is now LRU
	cache.Get("C")
	cache.Put("F", "Felp")
	assert.Nil(t, cache.Get("D"))
	assert.Equal(t, 4, cache.Size())

	cache.Delete("A")
	assert.Nil(t, cache.Get("A"))
}

func TestGenerics(t *testing.T) {
	key := keyType{
		dummyString: "some random key",
		dummyInt:    59,
	}
	value := "some random value"

	cache := New(&Options{MaxCount: 5}, nil)
	cache.Put(key, value)

	assert.Equal(t, value, cache.Get(key))
	assert.Equal(t, value, cache.Get(keyType{
		dummyString: "some random key",
		dummyInt:    59,
	}))
	assert.Nil(t, cache.Get(keyType{
		dummyString: "some other random key",
		dummyInt:    56,
	}))
}

func TestLRUWithTTL(t *testing.T) {
	mockTimeSource := clock.NewMockedTimeSourceAt(time.UnixMilli(0))
	cache := New(&Options{
		MaxCount:   5,
		TTL:        time.Millisecond * 100,
		TimeSource: mockTimeSource,
	}, nil).(*lru)

	cache.Put("A", "foo")
	assert.Equal(t, "foo", cache.Get("A"))

	mockTimeSource.Advance(time.Millisecond * 300)

	assert.Nil(t, cache.Get("A"))
	assert.Equal(t, 0, cache.Size())
}

func TestLRUCacheConcurrentAccess(t *testing.T) {
	cache := New(&Options{MaxCount: 5}, nil)
	values := map[string]string{
		"A": "foo",
		"B": "bar",
		"C": "zed",
		"D": "dank",
		"E": "ezpz",
	}

	for k, v := range values {
		cache.Put(k, v)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)

		// concurrent get and put
		go func() {
			defer wg.Done()

			<-start

			for j := 0; j < 1000; j++ {
				cache.Get("A")
				cache.Put("A", "fooo")
			}
		}()

		// concurrent iteration
		go func() {
			defer wg.Done()

			<-start

			for j := 0; j < 50; j++ {
				var result []Entry
				it := cache.Iterator()
				for it.HasNext() {
					entry := it.Next()
					result = append(result, entry) //nolint:staticcheck
				}
				it.Close()
			}
		}()
	}

	close(start)
	wg.Wait()
}

func TestRemoveFunc(t *testing.T) {
	ch := make(chan bool)
	cache := New(&Options{
		MaxCount: 5,
		RemovedFunc: func(i interface{}) {
			_, ok := i.(*testing.T)
			assert.True(t, ok)
			ch <- true
		},
	}, nil)

	cache.Put("testing", t)
	cache.Delete("testing")
	assert.Nil(t, cache.Get("testing"))

	timeout := time.NewTimer(time.Millisecond * 300)
	select {
	case b := <-ch:
		assert.True(t, b)
	case <-timeout.C:
		t.Error("RemovedFunc did not send true on channel ch")
	}
}

func TestRemovedFuncWithTTL(t *testing.T) {
	ch := make(chan bool)
	mockTimeSource := clock.NewMockedTimeSourceAt(time.UnixMilli(0))
	cache := New(&Options{
		MaxCount: 5,
		TTL:      time.Millisecond * 50,
		RemovedFunc: func(i interface{}) {
			_, ok := i.(*testing.T)
			assert.True(t, ok)
			ch <- true
		},
		TimeSource: mockTimeSource,
	}, nil).(*lru)

	cache.Put("A", t)
	assert.Equal(t, t, cache.Get("A"))

	mockTimeSource.Advance(time.Millisecond * 100)

	assert.Nil(t, cache.Get("A"))

	select {
	case b := <-ch:
		assert.True(t, b)
	case <-mockTimeSource.After(100 * time.Millisecond):
		t.Error("RemovedFunc did not send true on channel ch")
	}
}

func TestRemovedFuncWithTTL_Pin(t *testing.T) {
	ch := make(chan bool)
	mockTimeSource := clock.NewMockedTimeSourceAt(time.UnixMilli(0))
	cache := New(&Options{
		MaxCount: 5,
		TTL:      time.Millisecond * 50,
		Pin:      true,
		RemovedFunc: func(i interface{}) {
			_, ok := i.(*testing.T)
			assert.True(t, ok)
			ch <- true
		},
		TimeSource: mockTimeSource,
	}, nil).(*lru)

	_, err := cache.PutIfNotExist("A", t)
	assert.NoError(t, err)
	assert.Equal(t, t, cache.Get("A"))
	mockTimeSource.Advance(time.Millisecond * 100)
	assert.Equal(t, t, cache.Get("A"))
	// release 3 time since put if not exist also increase the counter
	cache.Release("A")
	cache.Release("A")
	cache.Release("A")
	assert.Nil(t, cache.Get("A"))

	select {
	case b := <-ch:
		assert.True(t, b)
	case <-mockTimeSource.After(300 * time.Millisecond):
		t.Error("RemovedFunc did not send true on channel ch")
	}
}

func TestIterator(t *testing.T) {
	expected := map[string]string{
		"A": "Alpha",
		"B": "Beta",
		"G": "Gamma",
		"D": "Delta",
	}

	cache := New(&Options{MaxCount: 5}, nil)

	for k, v := range expected {
		cache.Put(k, v)
	}

	actual := map[string]string{}

	it := cache.Iterator()
	for it.HasNext() {
		entry := it.Next()
		actual[entry.Key().(string)] = entry.Value().(string)
	}
	it.Close()
	assert.Equal(t, expected, actual)

	it = cache.Iterator()
	for i := 0; i < len(expected); i++ {
		entry := it.Next()
		actual[entry.Key().(string)] = entry.Value().(string)
	}
	it.Close()
	assert.Equal(t, expected, actual)
}

// Move the struct definition and method outside the test function
type sizeableValue struct {
	val  string
	size uint64
}

func (s sizeableValue) ByteSize() uint64 {
	return s.size
}

func TestLRU_SizeBased_SizeExceeded(t *testing.T) {
	cache := New(&Options{
		MaxCount:    5,
		IsSizeBased: true,
		MaxSize:     dynamicconfig.GetIntPropertyFn(15),
	}, nil)

	fooValue := sizeableValue{val: "Foo", size: 5}
	cache.Put("A", fooValue)
	assert.Equal(t, fooValue, cache.Get("A"))
	assert.Nil(t, cache.Get("B"))
	assert.Equal(t, 1, cache.Size())

	barValue := sizeableValue{val: "Bar", size: 5}
	cidValue := sizeableValue{val: "Cid", size: 5}
	deltValue := sizeableValue{val: "Delt", size: 5}

	cache.Put("B", barValue)
	cache.Put("C", cidValue)
	cache.Put("D", deltValue)
	assert.Nil(t, cache.Get("A"))
	assert.Equal(t, 3, cache.Size())

	assert.Equal(t, barValue, cache.Get("B"))
	assert.Equal(t, cidValue, cache.Get("C"))
	assert.Equal(t, deltValue, cache.Get("D"))

	foo2Value := sizeableValue{val: "Foo2", size: 5}
	cache.Put("A", foo2Value)
	assert.Equal(t, foo2Value, cache.Get("A"))
	assert.Nil(t, cache.Get("B"))
	assert.Equal(t, 3, cache.Size())

	// Put large value to evict the rest in a loop
	epsiValue := sizeableValue{val: "Epsi", size: 15}
	cache.Put("E", epsiValue)
	assert.Nil(t, cache.Get("C"))
	assert.Equal(t, epsiValue, cache.Get("E"))
	assert.Nil(t, cache.Get("A"))
	assert.Equal(t, 1, cache.Size())

	// Put large value greater than maxSize to evict everything
	mepsiValue := sizeableValue{val: "Mepsi", size: 25}
	cache.Put("M", mepsiValue)
	assert.Nil(t, cache.Get("M"))
	assert.Equal(t, 0, cache.Size())
}

func TestLRU_SizeBased_CountExceeded(t *testing.T) {
	cache := New(&Options{
		MaxCount:    5,
		IsSizeBased: true,
		MaxSize:     dynamicconfig.GetIntPropertyFn(10000),
	}, nil)

	fooValue := sizeableValue{val: "Foo", size: 5}
	cache.Put("A", fooValue)
	assert.Equal(t, fooValue, cache.Get("A"))
	assert.Nil(t, cache.Get("B"))
	assert.Equal(t, 1, cache.Size())

	barValue := sizeableValue{val: "Bar", size: 5}
	cidValue := sizeableValue{val: "Cid", size: 5}
	deltValue := sizeableValue{val: "Delt", size: 5}

	cache.Put("B", barValue)
	cache.Put("C", cidValue)
	cache.Put("D", deltValue)
	assert.Equal(t, 4, cache.Size())

	assert.Equal(t, barValue, cache.Get("B"))
	assert.Equal(t, cidValue, cache.Get("C"))
	assert.Equal(t, deltValue, cache.Get("D"))

	foo2Value := sizeableValue{val: "Foo2", size: 5}
	cache.Put("A", foo2Value)
	assert.Equal(t, foo2Value, cache.Get("A"))
	assert.Equal(t, 4, cache.Size())

	epsiValue := sizeableValue{val: "Epsi", size: 5}
	cache.Put("E", epsiValue)
	assert.Equal(t, barValue, cache.Get("B"))
	assert.Equal(t, epsiValue, cache.Get("E"))
	assert.Equal(t, foo2Value, cache.Get("A"))
	assert.Equal(t, 5, cache.Size())
}

func TestPanicMaxCountAndSizeNotProvided(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The LRU was initialized without panic")
		}
	}()

	New(&Options{
		TTL: time.Millisecond * 100,
		GetCacheItemSizeFunc: func(interface{}) uint64 {
			return 5
		},
	}, nil)
}

func TestPanicMaxCountAndSizeFuncNotProvided(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The LRU was initialized without panic")
		}
	}()

	New(&Options{
		TTL:     time.Millisecond * 100,
		MaxSize: dynamicconfig.GetIntPropertyFn(25),
	}, nil)
}

func TestPanicOptionsIsNil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The LRU was initialized without panic")
		}
	}()

	New(nil, nil)
}

func TestEvictItemsPastTimeToLive_ActivelyEvict(t *testing.T) {
	// Create the cache with a TTL of 75s
	mockTimeSource := clock.NewMockedTimeSourceAt(time.UnixMilli(0))
	cache, ok := New(&Options{
		MaxCount:      5,
		TTL:           time.Second * 75,
		ActivelyEvict: true,
		TimeSource:    mockTimeSource,
	}, nil).(*lru)
	require.True(t, ok)

	_, err := cache.PutIfNotExist("A", 1)
	require.NoError(t, err)
	_, err = cache.PutIfNotExist("B", 2)
	require.NoError(t, err)

	// Nothing is expired after 50s
	mockTimeSource.Advance(time.Second * 50)
	assert.Equal(t, 2, cache.Size())

	_, err = cache.PutIfNotExist("C", 3)
	require.NoError(t, err)
	_, err = cache.PutIfNotExist("D", 4)
	require.NoError(t, err)

	// No time has passed, so still nothing is expired
	assert.Equal(t, 4, cache.Size())

	// Advance time to 100s, so A and B should be expired
	mockTimeSource.Advance(time.Second * 50)
	assert.Equal(t, 2, cache.Size())

	// Advance time to 150s, so C and D should be expired as well
	mockTimeSource.Advance(time.Second * 50)
	assert.Equal(t, 0, cache.Size())
}

func TestUpdateSizeOnReplace(t *testing.T) {
	// Create a size-based LRU cache
	cache := &lru{
		isSizeBased: true,
		sizeByKey:   make(map[interface{}]uint64),
		currSize:    0,
	}

	// Test Case 1: Replace with a key that doesn't exist yet
	key1 := "key1"
	initialSize := cache.currSize
	size1 := uint64(100)

	prevSize := cache.updateSizeOnReplace(key1, size1)
	assert.Equal(t, uint64(0), prevSize, "Previous size should be 0 for a new key")
	assert.Equal(t, size1, cache.sizeByKey[key1], "Size for key1 should be updated in sizeByKey")
	assert.Equal(t, initialSize+size1, cache.currSize, "Current size should increase by size1")

	// Test Case 2: Replace with a key that already exists
	size2 := uint64(200)
	prevSize = cache.updateSizeOnReplace(key1, size2)
	assert.Equal(t, size1, prevSize, "Previous size should be size1")
	assert.Equal(t, size2, cache.sizeByKey[key1], "Size for key1 should be updated to size2")
	assert.Equal(t, initialSize+size2, cache.currSize, "Current size should be updated correctly")

	// Test Case 3: Test with multiple keys
	key2 := "key2"
	size3 := uint64(300)
	prevSize = cache.updateSizeOnReplace(key2, size3)
	assert.Equal(t, uint64(0), prevSize, "Previous size should be 0 for a new key")
	assert.Equal(t, size3, cache.sizeByKey[key2], "Size for key2 should be updated in sizeByKey")
	assert.Equal(t, initialSize+size2+size3, cache.currSize, "Current size should increase by size3")
}

// TestPutInternalWithFullCache tests the scenario where we need to revert size changes due to a full cache
func TestPutInternalWithFullCache(t *testing.T) {
	mockTimeSource := clock.NewMockedTimeSourceAt(time.UnixMilli(0))

	// Create a size-based LRU cache with a small max size
	cache := &lru{
		byAccess:    list.New(),
		byKey:       make(map[interface{}]*list.Element),
		isSizeBased: true,
		sizeByKey:   make(map[interface{}]uint64),
		currSize:    0,
		maxSize:     dynamicconfig.GetIntPropertyFn(500),
		pin:         true, // Enable pinning for this test
		timeSource:  mockTimeSource,
		logger:      log.NewNoop(),
	}

	// Add an initial item that takes up most of the cache and pin it
	key1 := "key1"
	value1 := sizeableValue{val: "value1", size: 400}
	_, err := cache.putInternal(key1, value1, true)
	assert.NoError(t, err)
	assert.Equal(t, uint64(400), cache.currSize)

	// Get the item to increment the refCount (pin it)
	cache.Get(key1)

	// Try to update with a larger value, which would make the cache full
	value1Updated := sizeableValue{val: "value1Updated", size: 600}

	// This should return ErrCacheFull and revert the size change
	existingValue, err := cache.putInternal(key1, value1Updated, true)

	// Verify that the error is ErrCacheFull
	assert.Equal(t, ErrCacheFull, err)

	// Verify that the value was not updated
	assert.Equal(t, value1, existingValue)

	// Verify that the size was reverted back to original
	assert.Equal(t, uint64(400), cache.currSize)
	assert.Equal(t, uint64(400), cache.sizeByKey[key1])

	// Verify the item in the cache wasn't changed
	cachedValue := cache.Get(key1)
	assert.Equal(t, value1, cachedValue)
}

func TestPutInternalUpdateExistingKey(t *testing.T) {
	// Setup a mock time source
	mockTimeSource := clock.NewMockedTimeSourceAt(time.UnixMilli(0))

	// Create a size-based LRU cache
	cache := &lru{
		byAccess:    list.New(),
		byKey:       make(map[interface{}]*list.Element),
		isSizeBased: true,
		sizeByKey:   make(map[interface{}]uint64),
		currSize:    0,
		maxSize:     dynamicconfig.GetIntPropertyFn(1000),
		timeSource:  mockTimeSource,
		logger:      log.NewNoop(),
	}

	// Test updating an existing key with allowUpdate=true

	// First, add an initial item
	key1 := "key1"
	initialValue := sizeableValue{val: "initialValue", size: 100}

	// Add the initial item
	_, err := cache.putInternal(key1, initialValue, true)
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), cache.currSize)

	// Now update the item with a different value but same size
	updatedValue := sizeableValue{val: "updatedValue", size: 100}
	returnedValue, err := cache.putInternal(key1, updatedValue, true)

	// Verify the returned value is the initial value
	assert.NoError(t, err)
	assert.Equal(t, initialValue, returnedValue)

	// Verify the cache contains the updated value
	cachedValue := cache.Get(key1)
	assert.Equal(t, updatedValue, cachedValue)
	assert.Equal(t, uint64(100), cache.currSize)

	// Now update with a larger value to test size change
	largerValue := sizeableValue{val: "largerValue", size: 200}
	returnedValue, err = cache.putInternal(key1, largerValue, true)

	// Verify the returned value is the updated value
	assert.NoError(t, err)
	assert.Equal(t, updatedValue, returnedValue)

	// Verify the cache contains the larger value and size is updated
	cachedValue = cache.Get(key1)
	assert.Equal(t, largerValue, cachedValue)
	assert.Equal(t, uint64(200), cache.currSize)

	// Test with TTL functionality
	cache.ttl = time.Hour

	// Record the current time
	now := mockTimeSource.Now()

	// Update again to refresh the creation time
	newerValue := sizeableValue{val: "newerValue", size: 150}
	_, err = cache.putInternal(key1, newerValue, true)
	assert.NoError(t, err)

	// Get the element from byKey to check its creation time
	element := cache.byKey[key1]
	entry := element.Value.(*entryImpl)

	// Verify the creation time was updated
	assert.True(t, entry.createTime.After(now) || entry.createTime.Equal(now),
		"Creation time should be updated to current time")

	// Test with allowUpdate=false
	// The value should not be updated
	finalValue := sizeableValue{val: "finalValue", size: 300}
	returnedValue, err = cache.putInternal(key1, finalValue, false)

	// Verify the returned value is the newer value and no error
	assert.NoError(t, err)
	assert.Equal(t, newerValue, returnedValue)

	// Verify the cache still contains the newer value, not the final value
	cachedValue = cache.Get(key1)
	assert.Equal(t, newerValue, cachedValue)
	assert.Equal(t, uint64(150), cache.currSize)
}
