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

package tokenbucket

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
)

type (
	// TokenBucket is the interface for any implementation of a token bucket rate limiter
	TokenBucket interface {
		// TryConsume attempts to take count tokens from the
		// bucket. Returns true on success, false
		// otherwise along with the duration for the next refill
		TryConsume(count int) (bool, time.Duration)
		// Consume waits up to timeout duration to take count
		// tokens from the bucket. Returns true if count
		// tokens were acquired before timeout, false
		// otherwise
		Consume(count int, timeout time.Duration) bool
	}

	// PriorityTokenBucket is the interface for rate limiter with priority
	PriorityTokenBucket interface {
		// GetToken attempts to take count tokens from the
		// bucket with that priority. Priority 0 is highest.
		// Returns true on success, false
		// otherwise along with the duration for the next refill
		GetToken(priority, count int) (bool, time.Duration)
	}

	tokenBucketImpl struct {
		sync.Mutex
		tokens       int
		fillRate     int           // amount of tokens to add every interval
		fillInterval time.Duration // time between refills
		// Because we divide the per-second quota equally
		// every 100 millis, there could be a remainder when
		// the desired rate is not a multiple 10 (1second/100Millis)
		// To overcome this, we keep track of left over remainder
		// and distribute this evenly during every fillInterval
		overflowRps            int
		overflowTokens         int
		nextRefillTime         time.Time
		nextOverflowRefillTime time.Time
		clock                  clock.TimeSource
	}

	dynamicTokenBucketImpl struct {
		tb         *tokenBucketImpl
		currentRPS int32
		rps        dynamicproperties.IntPropertyFn
	}

	priorityTokenBucketImpl struct {
		sync.Mutex
		tokens         []int
		fillRate       int
		nextRefillTime time.Time
		// Because we divide the per-second quota equally
		// every 100 millis, there could be a remainder when
		// the desired rate is not a multiple 10 (1second/100Millis)
		// To overcome this, we keep track of left over remainder
		// and distribute this evenly during every fillInterval
		overflowRps            int
		overflowTokens         int
		nextOverflowRefillTime time.Time
		timeSource             clock.TimeSource
	}
)

const (
	millisPerSecond = 1000
	backoffInterval = 10 * time.Millisecond
	refillRate      = 100 * time.Millisecond
)

// New creates and returns a
// new token bucket rate limiter that
// replenishes the bucket every 100
// milliseconds. Thread safe.
//
// @param rps
//
//	Desired rate per second
//
// Golang.org has an alternative implementation
// of the rate limiter. On benchmarking, golang's
// implementation was order of magnitude slower.
// In addition, it does a lot more than what we
// need. These are the benchmarks under different
// scenarios
//
// BenchmarkTokenBucketParallel	50000000	        40.7 ns/op
// BenchmarkGolangRateParallel 	10000000	       150 ns/op
// BenchmarkTokenBucketParallel-8	20000000	       124 ns/op
// BenchmarkGolangRateParallel-8 	10000000	       208 ns/op
// BenchmarkTokenBucketParallel	50000000	        37.8 ns/op
// BenchmarkGolangRateParallel 	10000000	       153 ns/op
// BenchmarkTokenBucketParallel-8	10000000	       129 ns/op
// BenchmarkGolangRateParallel-8 	10000000	       208 ns/op
func New(rps int, timeSource clock.TimeSource) TokenBucket {
	return newTokenBucket(rps, timeSource)
}

func newTokenBucket(rps int, timeSource clock.TimeSource) *tokenBucketImpl {
	tb := new(tokenBucketImpl)
	tb.clock = timeSource
	tb.reset(rps)
	return tb
}

func (tb *tokenBucketImpl) TryConsume(count int) (bool, time.Duration) {
	now := tb.clock.Now()
	tb.Lock()
	tb.refill(now)
	nextRefillTime := tb.nextRefillTime.Sub(now)
	if tb.tokens < count {
		tb.Unlock()
		return false, nextRefillTime
	}
	tb.tokens -= count
	tb.Unlock()
	return true, nextRefillTime
}

func (tb *tokenBucketImpl) Consume(count int, timeout time.Duration) bool {

	var remTime = timeout
	var expiryTime = tb.clock.Now().Add(timeout)

	for {

		if ok, _ := tb.TryConsume(count); ok {
			return true
		}

		if remTime < backoffInterval {
			tb.clock.Sleep(remTime)
		} else {
			tb.clock.Sleep(backoffInterval)
		}

		now := tb.clock.Now()
		if now.Compare(expiryTime) >= 0 {
			return false
		}

		remTime = expiryTime.Sub(now)
	}
}

func (tb *tokenBucketImpl) reset(rps int) {
	tb.Lock()
	tb.fillInterval = refillRate
	tb.fillRate = (rps * 100) / millisPerSecond
	tb.overflowRps = rps - (10 * tb.fillRate)
	tb.nextOverflowRefillTime = time.Time{}
	tb.Unlock()
}

func (tb *tokenBucketImpl) refill(now time.Time) {
	tb.refillOverFlow(now)
	if tb.isRefillDue(now) {
		tb.tokens = tb.fillRate
		if tb.overflowTokens > 0 {
			tb.tokens++
			tb.overflowTokens--
		}
		tb.nextRefillTime = now.Add(tb.fillInterval)
	}
}

func (tb *tokenBucketImpl) refillOverFlow(now time.Time) {
	if tb.overflowRps < 1 {
		return
	}
	if tb.isOverflowRefillDue(now) {
		tb.overflowTokens = tb.overflowRps
		tb.nextOverflowRefillTime = now.Add(time.Second)
	}
}

func (tb *tokenBucketImpl) isRefillDue(now time.Time) bool {
	return now.Compare(tb.nextRefillTime) >= 0
}

func (tb *tokenBucketImpl) isOverflowRefillDue(now time.Time) bool {
	return now.Compare(tb.nextOverflowRefillTime) >= 0
}

// NewDynamicTokenBucket creates and returns a token bucket
// rate limiter that supports dynamic change of RPS. Thread safe.
// @param rps
//
//	Dynamic config function for rate per second
func NewDynamicTokenBucket(rps dynamicproperties.IntPropertyFn, timeSource clock.TimeSource) TokenBucket {
	initialRPS := rps()
	return &dynamicTokenBucketImpl{
		rps:        rps,
		currentRPS: int32(initialRPS),
		tb:         newTokenBucket(initialRPS, timeSource),
	}
}

func (dtb *dynamicTokenBucketImpl) TryConsume(count int) (bool, time.Duration) {
	dtb.resetRateIfChanged(dtb.rps())
	return dtb.tb.TryConsume(count)
}

func (dtb *dynamicTokenBucketImpl) Consume(count int, timeout time.Duration) bool {
	dtb.resetRateIfChanged(dtb.rps())
	return dtb.tb.Consume(count, timeout)
}

// resetLimitIfChanged resets the underlying token bucket if the
// current rps quota is different from the actual rps quota obtained
// from dynamic config
func (dtb *dynamicTokenBucketImpl) resetRateIfChanged(newRPS int) {
	currentRPS := atomic.LoadInt32(&dtb.currentRPS)
	if int(currentRPS) == newRPS {
		return
	}
	if atomic.CompareAndSwapInt32(&dtb.currentRPS, currentRPS, int32(newRPS)) {
		dtb.tb.reset(newRPS)
	}
}

// NewPriorityTokenBucket creates and returns a
// new token bucket rate limiter support priority.
// There are n buckets for n priorities. It
// replenishes the top priority bucket every 100
// milliseconds, unused tokens flows to next bucket.
// The idea comes from Dual Token Bucket Algorithms.
// Thread safe.
//
// @param numOfPriority
//
//	Number of priorities
//
// @param rps
//
//	Desired rate per second
func NewPriorityTokenBucket(numOfPriority, rps int, timeSource clock.TimeSource) PriorityTokenBucket {
	tb := new(priorityTokenBucketImpl)
	tb.tokens = make([]int, numOfPriority)
	tb.timeSource = timeSource
	tb.fillRate = (rps * 100) / millisPerSecond
	tb.overflowRps = rps - (10 * tb.fillRate)
	tb.refill(tb.timeSource.Now())
	return tb
}

// NewFullPriorityTokenBucket creates and returns a new priority token bucket with all bucket init with full tokens.
// With all buckets full, get tokens from low priority buckets won't be missed initially, but may caused bursts.
func NewFullPriorityTokenBucket(numOfPriority, rps int, timeSource clock.TimeSource) PriorityTokenBucket {
	tb := new(priorityTokenBucketImpl)
	tb.tokens = make([]int, numOfPriority)
	tb.timeSource = timeSource
	tb.fillRate = (rps * 100) / millisPerSecond
	tb.overflowRps = rps - (10 * tb.fillRate)
	tb.refill(tb.timeSource.Now())
	for i := 1; i < numOfPriority; i++ {
		tb.nextRefillTime = time.Time{}
		tb.refill(tb.timeSource.Now())
	}
	return tb
}

func (tb *priorityTokenBucketImpl) GetToken(priority, count int) (bool, time.Duration) {
	now := tb.timeSource.Now()
	tb.Lock()
	tb.refill(now)
	nextRefillTime := tb.nextRefillTime.Sub(now)
	if tb.tokens[priority] < count {
		tb.Unlock()
		return false, nextRefillTime
	}
	tb.tokens[priority] -= count
	tb.Unlock()
	return true, nextRefillTime
}

func (tb *priorityTokenBucketImpl) refill(now time.Time) {
	tb.refillOverFlow(now)
	if tb.isRefillDue(now) {
		more := tb.fillRate
		for i := 0; i < len(tb.tokens); i++ {
			tb.tokens[i] += more
			if tb.tokens[i] > tb.fillRate {
				more = tb.tokens[i] - tb.fillRate
				tb.tokens[i] = tb.fillRate
			} else {
				break
			}
		}
		if tb.overflowTokens > 0 {
			tb.tokens[0]++
			tb.overflowTokens--
		}
		tb.nextRefillTime = now.Add(refillRate)
	}
}

func (tb *priorityTokenBucketImpl) refillOverFlow(now time.Time) {
	if tb.overflowRps < 1 {
		return
	}
	if tb.isOverflowRefillDue(now) {
		tb.overflowTokens = tb.overflowRps
		tb.nextOverflowRefillTime = now.Add(time.Second)
	}
}

func (tb *priorityTokenBucketImpl) isRefillDue(now time.Time) bool {
	return now.Compare(tb.nextRefillTime) >= 0
}

func (tb *priorityTokenBucketImpl) isOverflowRefillDue(now time.Time) bool {
	return now.Compare(tb.nextOverflowRefillTime) >= 0
}
