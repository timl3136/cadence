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

package poller

import (
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/types"
)

const (
	pollerHistoryInitSize    = 0
	pollerHistoryInitMaxSize = 5000
	pollerHistoryTTL         = 5 * time.Minute
)

type (
	Identity string

	Info struct {
		RatePerSecond  float64
		IsolationGroup string
	}
)

type History struct {
	// poller ID -> pollerInfo
	// pollers map[pollerID]pollerInfo
	history cache.Cache

	// OnHistoryUpdatedFunc is a function called when the poller history was updated
	onHistoryUpdatedFunc HistoryUpdatedFunc
}

// HistoryUpdatedFunc is a type for notifying applications when the poller history was updated
type HistoryUpdatedFunc func()

func NewPollerHistory(historyUpdatedFunc HistoryUpdatedFunc) *History {
	opts := &cache.Options{
		InitialCapacity: pollerHistoryInitSize,
		TTL:             pollerHistoryTTL,
		Pin:             false,
		MaxCount:        pollerHistoryInitMaxSize,
	}

	return &History{
		history:              cache.New(opts),
		onHistoryUpdatedFunc: historyUpdatedFunc,
	}
}

func (pollers *History) UpdatePollerInfo(id Identity, info Info) {
	pollers.history.Put(id, &info)
	if pollers.onHistoryUpdatedFunc != nil {
		pollers.onHistoryUpdatedFunc()
	}
}

func (pollers *History) GetPollerInfo(earliestAccessTime time.Time) []*types.PollerInfo {
	var result []*types.PollerInfo

	ite := pollers.history.Iterator()
	defer ite.Close()
	for ite.HasNext() {
		entry := ite.Next()
		key := entry.Key().(Identity)
		value := entry.Value().(*Info)
		// TODO add IP, T1396795
		lastAccessTime := entry.CreateTime()
		if earliestAccessTime.Before(lastAccessTime) {
			result = append(result, &types.PollerInfo{
				Identity:       string(key),
				LastAccessTime: common.Int64Ptr(lastAccessTime.UnixNano()),
				RatePerSecond:  value.RatePerSecond,
			})
		}
	}

	return result
}

func (pollers *History) GetPollerIsolationGroups(earliestAccessTime time.Time) map[string]struct{} {
	groupSet := make(map[string]struct{})
	ite := pollers.history.Iterator()
	defer ite.Close()
	for ite.HasNext() {
		entry := ite.Next()
		value := entry.Value().(*Info)
		lastAccessTime := entry.CreateTime()
		if earliestAccessTime.Before(lastAccessTime) {
			if value.IsolationGroup != "" {
				groupSet[value.IsolationGroup] = struct{}{}
			}
		}
	}
	return groupSet
}
