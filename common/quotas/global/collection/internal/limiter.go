// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Package internal protects these types' concurrency primitives and other
// internals from accidental misuse.
package internal

import (
	"math"
	"sync/atomic"

	"golang.org/x/time/rate"
)

type (
	// Limiter is a simplified version of [github.com/uber/cadence/common/quotas.Limiter],
	// for both simpler mocking and to remove the need to import it.
	//
	// Wait and Reserve are intentionally excluded here.  They are not currently needed,
	// and it seems likely to be much more difficult to correctly track their usage.
	Limiter interface {
		Allow() bool
	}

	// FallbackLimiter wraps a [rate.Limiter] with a fallback Limiter (i.e. a [github.com/uber/cadence/common/quotas.Limiter])
	// to use after a configurable number of failed updates.
	//
	// Intended use is:
	//   - collect allowed vs rejected metrics (implicitly tracked by calling Allow())
	//   - periodically, the limiting host gathers all FallbackLimiter metrics and zeros them (with Collect())
	//   - this info is submitted to aggregating hosts, who compute new target RPS values
	//   - these new target values are used to adjust this ratelimiter (with Update(...))
	//
	// During this sequence, a requested limit may not be returned by an aggregator for two major reasons,
	// and will result in a FailedUpdate() call to shorten a "use fallback logic" fuse:
	//   - this limit is legitimately unused and no data exists for it
	//   - ring re-sharding led to losing track of the limit, and it is now owned by a host with insufficient data
	//
	// To mitigate the impact of the second case, "insufficient data" responses from aggregating hosts
	// (which are currently modeled as "no data") are temporarily ignored, and the previously-configured
	// update is used.  This gives the aggregating host time to fill in its data, and then the next cycle should
	// use "real" values that match actual usage.
	//
	// If no data has been returned for a sufficiently long time, the "main" ratelimit will be dropped, and
	// the fallback limit will be used exclusively.  This is intended as a safety fallback, e.g. during initial
	// rollout/rollback and outage scenarios, normal use is not expected to rely on it.
	FallbackLimiter struct {
		// usage / available-limit data cannot be gathered from rate.Limiter, sadly.
		// so it needs to be collected externally.
		//
		// note that these atomics are values, not pointers, to keep data access relatively local.
		// this requires that FallbackLimiter is used as a pointer, not a value, and is never copied.
		// if this is not done, `go vet` will produce an error:
		//     ❯ go vet -copylocks .
		//     # github.com/uber/cadence/common/quotas/global/limiter
		//     ./collection.go:30:27: func passes lock by value: github.com/uber/cadence/common/quotas/global/limiter/internal.FallbackLimiter contains sync/atomic.Int64 contains sync/atomic.noCopy
		// which is checked by `make lint` during CI.
		//
		// changing them to pointers will not change any semantics, so that should be fine if it becomes needed.

		// accepted-request usage counter
		accepted atomic.Int64
		// rejected-request usage counter
		rejected atomic.Int64
		// number of failed updates, for deciding when to use fallback
		failedUpdates atomic.Int64

		// ratelimiters in use

		// fallback used when failedUpdates exceeds maxFailedUpdates,
		// or prior to initial data from the global ratelimiter system.
		fallback Limiter
		// local-only limiter based global ratelimiter values.
		//
		// note that use and modification is NOT synchronized externally,
		// so updates and deciding when to use the fallback must be done carefully
		// to avoid undesired combinations when they interleave.
		limit *rate.Limiter
	}
)

const (
	// when failed updates exceeds this value, use the fallback
	maxFailedUpdates = 9

	// at startup / new limits in use, use the fallback logic.
	// that's expected / normal behavior as we have no data yet.
	//
	// positive values risk being interpreted as a "failure" though, so start deeply
	// negative so it can be identified as "still starting up".
	// min-int64 has more than enough room to "count" failed updates for eons
	// without becoming positive, so it should not risk being misinterpreted.
	initialFailedUpdates = math.MinInt64
)

func NewFallbackLimiter(fallback Limiter) *FallbackLimiter {
	l := &FallbackLimiter{
		fallback: fallback,
		limit:    rate.NewLimiter(0, 0), // 0 allows no requests, will be unused until we receive an update
	}
	l.failedUpdates.Store(initialFailedUpdates)
	return l
}

// Collect returns the current accepted/rejected values, and resets them to zero.
// Small bits of imprecise counting / time-bucketing due to concurrent limiting is expected and allowed,
// as it should be more than small enough in practice to not matter.
func (b *FallbackLimiter) Collect() (accepted int, rejected int, usingFallback bool) {
	accepted = int(b.accepted.Swap(0))
	rejected = int(b.rejected.Swap(0))
	return accepted, rejected, b.useFallback()
}

func (b *FallbackLimiter) useFallback() bool {
	failed := b.failedUpdates.Load()
	startingUp := failed < 0
	tooManyFailures := failed > maxFailedUpdates
	return startingUp || tooManyFailures
}

// Update adjusts the underlying "main" ratelimit, and resets the fallback fuse.
func (b *FallbackLimiter) Update(rps float64) {
	// caution: order here matters, to prevent potentially-old limiter values from being used
	// before they are updated.
	//
	// this is probably not going to be noticeable, but some users are sensitive to ANY
	// requests being ratelimited.  updating the fallback fuse last should be more reliable
	// in preventing that from happening when they would not otherwise be limited, e.g. with
	// the initial value of 0 burst, or if their rps was very low long ago and is now high.
	defer func() {
		// reset the use-fallback fuse, which may also (re)enable using the "main" limiter
		b.failedUpdates.Store(0)
	}()

	if b.limit.Limit() == rate.Limit(rps) {
		return
	}

	b.limit.SetLimit(rate.Limit(rps))
	b.limit.SetBurst(max(1, int(rps))) // 0 burst blocks all requests, so allow at least 1 and rely on rps to fill sanely
}

// FailedUpdate should be called when a limit fails to update from an aggregator,
// possibly implying some kind of problem, which may be unique to this limit.
//
// After crossing a threshold of failures (currently 10), the fallback will be switched to.
func (b *FallbackLimiter) FailedUpdate() (failures int) {
	failures = int(b.failedUpdates.Add(1)) // always increment the count for monitoring purposes
	return failures
}

// Reset defers to the fallback until an update is received.
//
// This is intended to be used when the current limit is no longer trustworthy for some reason,
// determined externally rather than from too many FailedUpdate calls.
func (b *FallbackLimiter) Reset() {
	b.failedUpdates.Store(initialFailedUpdates)
}

// Allow returns true if a request is allowed right now.
func (b *FallbackLimiter) Allow() bool {
	var allowed bool
	if b.useFallback() {
		allowed = b.fallback.Allow()
	} else {
		allowed = b.limit.Allow()
	}

	if allowed {
		b.accepted.Add(1)
	} else {
		b.rejected.Add(1)
	}
	return allowed
}

// intentionally shadows builtin max, so it can simply be deleted when 1.21 is adopted
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
