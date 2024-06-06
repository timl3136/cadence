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

package tasklist

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/partition"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/matching/config"
	"github.com/uber/cadence/service/matching/poller"
)

func defaultTestConfig() *config.Config {
	config := config.NewConfig(dynamicconfig.NewNopCollection(), "some random hostname")
	config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskListInfo(100 * time.Millisecond)
	config.MaxTaskDeleteBatchSize = dynamicconfig.GetIntPropertyFilteredByTaskListInfo(1)
	config.AllIsolationGroups = []string{"datacenterA", "datacenterB"}
	config.GetTasksBatchSize = dynamicconfig.GetIntPropertyFilteredByTaskListInfo(10)
	config.AsyncTaskDispatchTimeout = dynamicconfig.GetDurationPropertyFnFilteredByTaskListInfo(10 * time.Millisecond)
	return config
}

func TestDeliverBufferTasks(t *testing.T) {
	controller := gomock.NewController(t)
	logger := testlogger.New(t)

	tests := []func(tlm *taskListManagerImpl){
		func(tlm *taskListManagerImpl) { tlm.taskReader.cancelFunc() },
		func(tlm *taskListManagerImpl) {
			rps := 0.1
			tlm.matcher.UpdateRatelimit(&rps)
			tlm.taskReader.taskBuffers[defaultTaskBufferIsolationGroup] <- &persistence.TaskInfo{}
			_, err := tlm.matcher.ratelimit(context.Background()) // consume the token
			assert.NoError(t, err)
			tlm.taskReader.cancelFunc()
		},
	}
	for _, test := range tests {
		tlm := createTestTaskListManager(t, logger, controller)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			tlm.taskReader.dispatchBufferedTasks(defaultTaskBufferIsolationGroup)
		}()
		test(tlm)
		// dispatchBufferedTasks should stop after invocation of the test function
		wg.Wait()
	}
}

func TestDeliverBufferTasks_NoPollers(t *testing.T) {
	controller := gomock.NewController(t)
	logger := testlogger.New(t)

	tlm := createTestTaskListManager(t, logger, controller)
	tlm.taskReader.taskBuffers[defaultTaskBufferIsolationGroup] <- &persistence.TaskInfo{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tlm.taskReader.dispatchBufferedTasks("")
		wg.Done()
	}()
	time.Sleep(100 * time.Millisecond) // let go routine run first and block on tasksForPoll
	tlm.taskReader.cancelFunc()
	wg.Wait()
}

func TestReadLevelForAllExpiredTasksInBatch(t *testing.T) {
	controller := gomock.NewController(t)
	logger := testlogger.New(t)

	tlm := createTestTaskListManager(t, logger, controller)
	tlm.db.rangeID = int64(1)
	tlm.taskAckManager.SetAckLevel(0)
	tlm.taskAckManager.SetReadLevel(0)
	require.Equal(t, int64(0), tlm.taskAckManager.GetAckLevel())
	require.Equal(t, int64(0), tlm.taskAckManager.GetReadLevel())

	// Add all expired tasks
	tasks := []*persistence.TaskInfo{
		{
			TaskID:      11,
			Expiry:      time.Now().Add(-time.Minute),
			CreatedTime: time.Now().Add(-time.Hour),
		},
		{
			TaskID:      12,
			Expiry:      time.Now().Add(-time.Minute),
			CreatedTime: time.Now().Add(-time.Hour),
		},
	}

	require.True(t, tlm.taskReader.addTasksToBuffer(tasks))
	require.Equal(t, int64(0), tlm.taskAckManager.GetAckLevel())
	require.Equal(t, int64(12), tlm.taskAckManager.GetReadLevel())

	// Now add a mix of valid and expired tasks
	require.True(t, tlm.taskReader.addTasksToBuffer([]*persistence.TaskInfo{
		{
			TaskID:      13,
			Expiry:      time.Now().Add(-time.Minute),
			CreatedTime: time.Now().Add(-time.Hour),
		},
		{
			TaskID:      14,
			Expiry:      time.Now().Add(time.Hour),
			CreatedTime: time.Now().Add(time.Minute),
		},
	}))
	require.Equal(t, int64(0), tlm.taskAckManager.GetAckLevel())
	require.Equal(t, int64(14), tlm.taskAckManager.GetReadLevel())
}

func createTestTaskListManager(t *testing.T, logger log.Logger, controller *gomock.Controller) *taskListManagerImpl {
	return createTestTaskListManagerWithConfig(t, logger, controller, defaultTestConfig())
}

func createTestTaskListManagerWithConfig(t *testing.T, logger log.Logger, controller *gomock.Controller, cfg *config.Config) *taskListManagerImpl {
	tm := NewTestTaskManager(t, logger)
	mockPartitioner := partition.NewMockPartitioner(controller)
	mockPartitioner.EXPECT().GetIsolationGroupByDomainID(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	mockDomainCache := cache.NewMockDomainCache(controller)
	mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(cache.CreateDomainCacheEntry("domainName"), nil).AnyTimes()
	mockDomainCache.EXPECT().GetDomainName(gomock.Any()).Return("domainName", nil).AnyTimes()
	tl := "tl"
	dID := "domain"
	tlID, err := NewIdentifier(dID, tl, persistence.TaskListTypeActivity)
	if err != nil {
		panic(err)
	}
	tlKind := types.TaskListKindNormal
	tlMgr, err := NewManager(mockDomainCache, logger, metrics.NewClient(tally.NoopScope, metrics.Matching), tm, cluster.GetTestClusterMetadata(true), mockPartitioner, nil, func(Manager) {}, tlID, &tlKind, cfg, clock.NewRealTimeSource(), time.Now())
	if err != nil {
		logger.Fatal("error when createTestTaskListManager", tag.Error(err))
	}
	return tlMgr.(*taskListManagerImpl)
}

func TestDescribeTaskList(t *testing.T) {
	controller := gomock.NewController(t)
	logger := testlogger.New(t)

	startTaskID := int64(1)
	taskCount := int64(3)
	PollerIdentity := "test-poll"

	// Create taskList Manager and set taskList state
	tlm := createTestTaskListManager(t, logger, controller)
	tlm.db.rangeID = int64(1)
	tlm.taskAckManager.SetAckLevel(0)

	for i := int64(0); i < taskCount; i++ {
		err := tlm.taskAckManager.ReadItem(startTaskID + i)
		assert.Nil(t, err)
	}

	includeTaskStatus := false
	descResp := tlm.DescribeTaskList(includeTaskStatus)
	require.Equal(t, 0, len(descResp.GetPollers()))
	require.Nil(t, descResp.GetTaskListStatus())

	includeTaskStatus = true
	taskListStatus := tlm.DescribeTaskList(includeTaskStatus).GetTaskListStatus()
	require.NotNil(t, taskListStatus)
	require.Zero(t, taskListStatus.GetAckLevel())
	require.Equal(t, taskCount, taskListStatus.GetReadLevel())
	require.Equal(t, int64(0), taskListStatus.GetBacklogCountHint())
	require.InDelta(t, taskListStatus.GetRatePerSecond(), tlm.config.TaskDispatchRPS, 1.0)
	taskIDBlock := taskListStatus.GetTaskIDBlock()
	require.Equal(t, int64(1), taskIDBlock.GetStartID())
	require.Equal(t, tlm.config.RangeSize, taskIDBlock.GetEndID())

	// Add a poller and complete all tasks
	tlm.pollerHistory.UpdatePollerInfo(poller.Identity(PollerIdentity), poller.Info{RatePerSecond: tlm.config.TaskDispatchRPS})
	for i := int64(0); i < taskCount; i++ {
		tlm.taskAckManager.AckItem(startTaskID + i)
	}

	descResp = tlm.DescribeTaskList(includeTaskStatus)
	require.Equal(t, 1, len(descResp.GetPollers()))
	require.Equal(t, PollerIdentity, descResp.Pollers[0].GetIdentity())
	require.NotEmpty(t, descResp.Pollers[0].GetLastAccessTime())
	require.InDelta(t, descResp.Pollers[0].GetRatePerSecond(), tlm.config.TaskDispatchRPS, 1.0)

	rps := 5.0
	tlm.pollerHistory.UpdatePollerInfo(poller.Identity(PollerIdentity), poller.Info{RatePerSecond: rps})
	descResp = tlm.DescribeTaskList(includeTaskStatus)
	require.Equal(t, 1, len(descResp.GetPollers()))
	require.Equal(t, PollerIdentity, descResp.Pollers[0].GetIdentity())
	require.InDelta(t, descResp.Pollers[0].GetRatePerSecond(), rps, 1.0)

	taskListStatus = descResp.GetTaskListStatus()
	require.NotNil(t, taskListStatus)
	require.Equal(t, taskCount, taskListStatus.GetAckLevel())
	require.Zero(t, taskListStatus.GetBacklogCountHint())
}

func TestCheckIdleTaskList(t *testing.T) {
	cfg := config.NewConfig(dynamicconfig.NewNopCollection(), "some random hostname")
	cfg.IdleTasklistCheckInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskListInfo(10 * time.Millisecond)

	t.Run("Idle task-list", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		tlm := createTestTaskListManagerWithConfig(t, testlogger.New(t), ctrl, cfg)
		require.NoError(t, tlm.Start())

		require.EqualValues(t, 0, atomic.LoadInt32(&tlm.stopped), "idle check interval had not passed yet")
		time.Sleep(20 * time.Millisecond)
		require.EqualValues(t, 1, atomic.LoadInt32(&tlm.stopped), "idle check interval should have pass")
	})

	t.Run("Active poll-er", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		tlm := createTestTaskListManagerWithConfig(t, testlogger.New(t), ctrl, cfg)
		require.NoError(t, tlm.Start())

		time.Sleep(8 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _ = tlm.GetTask(ctx, nil)
		cancel()

		// task list manager should have been stopped,
		// but GetTask extends auto-stop until the next check-idle-task-list-interval
		time.Sleep(6 * time.Millisecond)
		require.EqualValues(t, 0, atomic.LoadInt32(&tlm.stopped))

		time.Sleep(20 * time.Millisecond)
		require.EqualValues(t, 1, atomic.LoadInt32(&tlm.stopped), "idle check interval should have pass")
	})

	t.Run("Active adding task", func(t *testing.T) {
		domainID := uuid.New()
		workflowID := uuid.New()
		runID := uuid.New()

		addTaskParam := AddTaskParams{
			Execution: &types.WorkflowExecution{
				WorkflowID: workflowID,
				RunID:      runID,
			},
			TaskInfo: &persistence.TaskInfo{
				DomainID:               domainID,
				WorkflowID:             workflowID,
				RunID:                  runID,
				ScheduleID:             2,
				ScheduleToStartTimeout: 5,
				CreatedTime:            time.Now(),
			},
		}

		ctrl := gomock.NewController(t)
		tlm := createTestTaskListManagerWithConfig(t, testlogger.New(t), ctrl, cfg)
		require.NoError(t, tlm.Start())

		time.Sleep(8 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := tlm.AddTask(ctx, addTaskParam)
		require.NoError(t, err)
		cancel()

		// task list manager should have been stopped,
		// but AddTask extends auto-stop until the next check-idle-task-list-interval
		time.Sleep(6 * time.Millisecond)
		require.EqualValues(t, 0, atomic.LoadInt32(&tlm.stopped))

		time.Sleep(20 * time.Millisecond)
		require.EqualValues(t, 1, atomic.LoadInt32(&tlm.stopped), "idle check interval should have pass")
	})
}

func TestAddTaskStandby(t *testing.T) {
	controller := gomock.NewController(t)
	logger := testlogger.New(t)

	cfg := config.NewConfig(dynamicconfig.NewNopCollection(), "some random hostname")
	cfg.IdleTasklistCheckInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskListInfo(10 * time.Millisecond)

	tlm := createTestTaskListManagerWithConfig(t, logger, controller, cfg)
	require.NoError(t, tlm.Start())

	// stop taskWriter so that we can check if there's any call to it
	// otherwise the task persist process is async and hard to test
	tlm.taskWriter.Stop()

	domainID := uuid.New()
	workflowID := "some random workflowID"
	runID := "some random runID"

	addTaskParam := AddTaskParams{
		Execution: &types.WorkflowExecution{
			WorkflowID: workflowID,
			RunID:      runID,
		},
		TaskInfo: &persistence.TaskInfo{
			DomainID:               domainID,
			WorkflowID:             workflowID,
			RunID:                  runID,
			ScheduleID:             2,
			ScheduleToStartTimeout: 5,
			CreatedTime:            time.Now(),
		},
	}

	testStandbyDomainEntry := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: domainID, Name: "some random domain name"},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		1234,
	)
	mockDomainCache := tlm.domainCache.(*cache.MockDomainCache)
	mockDomainCache.EXPECT().GetDomainByID(domainID).Return(testStandbyDomainEntry, nil).AnyTimes()

	syncMatch, err := tlm.AddTask(context.Background(), addTaskParam)
	require.Equal(t, errShutdown, err) // task writer was stopped above
	require.False(t, syncMatch)

	addTaskParam.ForwardedFrom = "from child partition"
	syncMatch, err = tlm.AddTask(context.Background(), addTaskParam)
	require.Error(t, err) // should not persist the task
	require.False(t, syncMatch)
}

func TestGetPollerIsolationGroup(t *testing.T) {
	controller := gomock.NewController(t)
	logger := testlogger.New(t)

	config := defaultTestConfig()
	config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskListInfo(30 * time.Second)
	tlm := createTestTaskListManagerWithConfig(t, logger, controller, config)

	bgCtx := ContextWithPollerID(context.Background(), "poller0")
	bgCtx = ContextWithIdentity(bgCtx, "id0")
	bgCtx = ContextWithIsolationGroup(bgCtx, config.AllIsolationGroups[0])
	ctx, cancel := context.WithTimeout(bgCtx, time.Second)
	_, err := tlm.GetTask(ctx, nil)
	cancel()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), ErrNoTasks.Error())

	// we should get isolation groups that showed up within last 10 seconds
	groups := tlm.getPollerIsolationGroups()
	assert.Equal(t, 1, len(groups))
	assert.Equal(t, config.AllIsolationGroups[0], groups[0])

	// after 10s, the poller from that isolation group are cleared from the poller history
	time.Sleep(10 * time.Second)
	groups = tlm.getPollerIsolationGroups()
	assert.Equal(t, 0, len(groups))

	// we should get isolation groups of outstanding pollers
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(bgCtx, time.Second*20)
		_, err := tlm.GetTask(ctx, nil)
		cancel()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), ErrNoTasks.Error())
	}()
	time.Sleep(11 * time.Second)
	groups = tlm.getPollerIsolationGroups()
	wg.Wait()
	assert.Equal(t, 1, len(groups))
	assert.Equal(t, config.AllIsolationGroups[0], groups[0])
}

// return a client side tasklist throttle error from the rate limiter.
// The expected behaviour is to retry
func TestRateLimitErrorsFromTasklistDispatch(t *testing.T) {
	controller := gomock.NewController(t)
	logger := testlogger.New(t)

	config := defaultTestConfig()
	config.EnableTasklistIsolation = func(domain string) bool { return true }
	tlm := createTestTaskListManagerWithConfig(t, logger, controller, config)

	tlm.taskReader.dispatchTask = func(ctx context.Context, task *InternalTask) error {
		return ErrTasklistThrottled
	}
	tlm.taskReader.getIsolationGroupForTask = func(ctx context.Context, info *persistence.TaskInfo) (string, error) { return "datacenterA", nil }

	breakDispatcher, breakRetryLoop := tlm.taskReader.dispatchSingleTaskFromBuffer("datacenterA", &persistence.TaskInfo{})
	assert.False(t, breakDispatcher)
	assert.False(t, breakRetryLoop)
}

// The intent of this test: SingleTaskDispatch is one of two places where tasks are written to
// the taskreader.taskBuffers channels. As such, it needs to take care to not accidentally
// hit the channel when it's full, as it'll block, causing a deadlock (due to both this dispatch
// and the async task pump blocking trying to read to the channel)
func TestSingleTaskDispatchDoesNotDeadlock(t *testing.T) {
	controller := gomock.NewController(t)
	logger := testlogger.New(t)

	config := defaultTestConfig()
	config.EnableTasklistIsolation = func(domain string) bool { return true }
	tlm := createTestTaskListManagerWithConfig(t, logger, controller, config)

	// mock a timeout to cause tasks to be rerouted
	tlm.taskReader.dispatchTask = func(ctx context.Context, task *InternalTask) error {
		return context.DeadlineExceeded
	}
	tlm.taskReader.getIsolationGroupForTask = func(ctx context.Context, info *persistence.TaskInfo) (string, error) {
		// force this, on the second read, to always reroute to the other DC
		return "datacenterB", nil
	}

	maxBufferSize := config.GetTasksBatchSize("", "", 0) - 1

	tlm.taskReader.logger = testlogger.New(t)

	for i := 0; i < maxBufferSize; i++ {
		breakDispatcher, breakRetryLoop := tlm.taskReader.dispatchSingleTaskFromBuffer("datacenterA", &persistence.TaskInfo{})
		assert.False(t, breakDispatcher, "dispatch isn't shutting down")
		assert.True(t, breakRetryLoop, "should be able to successfully dispatch all these tasks to the other isolation group")
	}
	// We should see them all being redirected
	assert.Len(t, tlm.taskReader.taskBuffers["datacenterB"], maxBufferSize)
	// we should see none in DC A (they're not being redirected here)
	assert.Len(t, tlm.taskReader.taskBuffers["datacenterA"], 0)

	// ok, and here we try and ensure that this *does not block
	// and instead complains and live-retries
	breakDispatcher, breakRetryLoop := tlm.taskReader.dispatchSingleTaskFromBuffer("datacenterA", &persistence.TaskInfo{})
	assert.False(t, breakDispatcher, "dispatch isn't shutting down")
	assert.False(t, breakRetryLoop, "in the event of a task being unable to be cross-dispatched, the expectation is that it'll keep retrying")

	// and to be certain and avoid any accidental off-by-one errors, one more time to just be sure
	breakDispatcher, breakRetryLoop = tlm.taskReader.dispatchSingleTaskFromBuffer("datacenterA", &persistence.TaskInfo{})
	assert.False(t, breakDispatcher, "dispatch isn't shutting down")
	assert.False(t, breakRetryLoop, "in the event of a task being unable to be cross-dispatched, the expectation is that it'll keep retrying")
}

// This is a bit of a strange unit-test as it's
// ensuring that invalid behaviour is handled defensively.
// It *should never be the case* that the isolation group tries to
// dispatch to a buffer that isn't there, however, if it does, we want to just
// log this, emit a metric and fallback to the default isolation group.
func TestMisconfiguredZoneDoesNotBlock(t *testing.T) {
	controller := gomock.NewController(t)
	logger := testlogger.New(t)

	config := defaultTestConfig()
	config.EnableTasklistIsolation = func(domain string) bool { return true }
	tlm := createTestTaskListManagerWithConfig(t, logger, controller, config)

	// mock a timeout to cause tasks to be rerouted
	tlm.taskReader.dispatchTask = func(ctx context.Context, task *InternalTask) error {
		return context.DeadlineExceeded
	}
	tlm.taskReader.getIsolationGroupForTask = func(ctx context.Context, info *persistence.TaskInfo) (string, error) {
		return "a misconfigured isolation group", nil
	}

	maxBufferSize := config.GetTasksBatchSize("", "", 0) - 1

	for i := 0; i < maxBufferSize; i++ {
		breakDispatcher, breakRetryLoop := tlm.taskReader.dispatchSingleTaskFromBuffer("datacenterA", &persistence.TaskInfo{})
		assert.False(t, breakDispatcher, "dispatch isn't shutting down")
		assert.True(t, breakRetryLoop, "should be able to successfully dispatch all these tasks to the default isolation group")
	}
	// We should see them all being redirected
	assert.Len(t, tlm.taskReader.taskBuffers[defaultTaskBufferIsolationGroup], maxBufferSize)
	// we should see none in DC A (they're not being redirected here)
	assert.Len(t, tlm.taskReader.taskBuffers["datacenterA"], 0)

	// ok, and here we try and ensure that this *does not block
	// and instead complains and live-retries
	breakDispatcher, breakRetryLoop := tlm.taskReader.dispatchSingleTaskFromBuffer("datacenterA", &persistence.TaskInfo{})
	assert.False(t, breakDispatcher, "dispatch isn't shutting down")
	assert.False(t, breakRetryLoop, "in the event of a task being unable to be cross-dispatched, the expectation is that it'll keep retrying")
}

func TestTaskWriterShutdown(t *testing.T) {
	controller := gomock.NewController(t)
	logger := testlogger.New(t)
	tlm := createTestTaskListManager(t, logger, controller)
	err := tlm.Start()
	assert.NoError(t, err)

	// stop the task writer explicitly
	tlm.taskWriter.Stop()

	// now attempt to add a task
	addParams := AddTaskParams{
		Execution: &types.WorkflowExecution{
			WorkflowID: "workflow1",
			RunID:      "run1",
		},
		TaskInfo: &persistence.TaskInfo{
			DomainID:               "domainId",
			RunID:                  "run1",
			WorkflowID:             "workflow1",
			ScheduleID:             5,
			ScheduleToStartTimeout: 1,
		},
	}
	_, err = tlm.AddTask(context.Background(), addParams)
	assert.Error(t, err)

	// test race
	tlm.taskWriter.stopped = 0
	_, err = tlm.AddTask(context.Background(), addParams)
	assert.Error(t, err)
	tlm.taskWriter.stopped = 1 // reset it back to old value
	tlm.Stop()
}

func TestTaskListManagerGetTaskBatch(t *testing.T) {
	const taskCount = 1200
	const rangeSize = 10
	controller := gomock.NewController(t)
	mockPartitioner := partition.NewMockPartitioner(controller)
	mockPartitioner.EXPECT().GetIsolationGroupByDomainID(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	mockDomainCache := cache.NewMockDomainCache(controller)
	mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(cache.CreateDomainCacheEntry("domainName"), nil).AnyTimes()
	mockDomainCache.EXPECT().GetDomainName(gomock.Any()).Return("domainName", nil).AnyTimes()
	logger := testlogger.New(t)
	tm := NewTestTaskManager(t, logger)
	taskListID := NewTestTaskListID(t, "domainId", "tl", 0)
	timeSource := clock.NewRealTimeSource()
	cfg := defaultTestConfig()
	cfg.RangeSize = rangeSize
	tlMgr, err := NewManager(
		mockDomainCache,
		logger,
		metrics.NewClient(tally.NoopScope, metrics.Matching),
		tm,
		cluster.GetTestClusterMetadata(true),
		mockPartitioner,
		nil,
		func(Manager) {},
		taskListID,
		types.TaskListKindNormal.Ptr(),
		cfg,
		timeSource,
		timeSource.Now(),
	)
	assert.NoError(t, err)
	tlm := tlMgr.(*taskListManagerImpl)
	err = tlm.Start()
	assert.NoError(t, err)

	// add taskCount tasks
	for i := int64(0); i < taskCount; i++ {
		scheduleID := i * 3
		addParams := AddTaskParams{
			Execution: &types.WorkflowExecution{
				WorkflowID: "workflow1",
				RunID:      "run1",
			},
			TaskInfo: &persistence.TaskInfo{
				DomainID:               "domainId",
				RunID:                  "run1",
				WorkflowID:             "workflow1",
				ScheduleID:             scheduleID,
				ScheduleToStartTimeout: 1,
			},
		}
		_, err = tlm.AddTask(context.Background(), addParams)
		assert.NoError(t, err)
	}
	assert.Equal(t, taskCount, tm.GetTaskCount(taskListID))

	// wait until all tasks are read by the task pump and enqeued into the in-memory buffer
	// at the end of this step, ackManager readLevel will also be equal to the buffer size
	expectedBufSize := common.MinInt(cap(tlm.taskReader.taskBuffers[defaultTaskBufferIsolationGroup]), taskCount)
	assert.True(t, awaitCondition(func() bool {
		return len(tlm.taskReader.taskBuffers[defaultTaskBufferIsolationGroup]) == expectedBufSize
	}, time.Second))

	// stop all goroutines that read / write tasks in the background
	// remainder of this test works with the in-memory buffer
	tlm.Stop()

	// SetReadLevel should NEVER be called without updating ackManager.outstandingTasks
	// This is only for unit test purpose
	tlm.taskAckManager.SetReadLevel(tlm.taskWriter.GetMaxReadLevel())
	tasks, readLevel, isReadBatchDone, err := tlm.taskReader.getTaskBatch()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(tasks))
	assert.Equal(t, readLevel, tlm.taskWriter.GetMaxReadLevel())
	assert.True(t, isReadBatchDone)

	tlm.taskAckManager.SetReadLevel(0)
	tasks, readLevel, isReadBatchDone, err = tlm.taskReader.getTaskBatch()
	assert.NoError(t, err)
	assert.Equal(t, rangeSize, len(tasks))
	assert.Equal(t, rangeSize, int(readLevel))
	assert.True(t, isReadBatchDone)

	// reset the ackManager readLevel to the buffer size and consume
	// the in-memory tasks by calling Poll API - assert ackMgr state
	// at the end
	tlm.taskAckManager.SetReadLevel(int64(expectedBufSize))

	// complete rangeSize events
	tlMgr, err = NewManager(
		mockDomainCache,
		logger,
		metrics.NewClient(tally.NoopScope, metrics.Matching),
		tm,
		cluster.GetTestClusterMetadata(true),
		mockPartitioner,
		nil,
		func(Manager) {},
		taskListID,
		types.TaskListKindNormal.Ptr(),
		cfg,
		timeSource,
		timeSource.Now(),
	)
	assert.NoError(t, err)
	tlm = tlMgr.(*taskListManagerImpl)
	err = tlm.Start()
	assert.NoError(t, err)
	for i := int64(0); i < rangeSize; i++ {
		task, err := tlm.GetTask(context.Background(), nil)
		if errors.Is(err, ErrNoTasks) {
			continue
		}
		assert.NoError(t, err)
		assert.NotNil(t, task)
		task.Finish(nil)
	}
	assert.Equal(t, taskCount-rangeSize, tm.GetTaskCount(taskListID))
	tasks, _, isReadBatchDone, err = tlm.taskReader.getTaskBatch()
	assert.NoError(t, err)
	assert.True(t, 0 < len(tasks) && len(tasks) <= rangeSize)
	assert.True(t, isReadBatchDone)
	tlm.Stop()
}

func TestTaskListManagerGetTaskBatch_ReadBatchDone(t *testing.T) {
	const rangeSize = 10
	const maxReadLevel = int64(120)
	config := defaultTestConfig()
	config.RangeSize = rangeSize
	controller := gomock.NewController(t)
	logger := testlogger.New(t)
	tlMgr := createTestTaskListManagerWithConfig(t, logger, controller, config)

	tlMgr.taskAckManager.SetReadLevel(0)
	atomic.StoreInt64(&tlMgr.taskWriter.maxReadLevel, maxReadLevel)
	tasks, readLevel, isReadBatchDone, err := tlMgr.taskReader.getTaskBatch()
	assert.Empty(t, tasks)
	assert.Equal(t, int64(rangeSize*10), readLevel)
	assert.False(t, isReadBatchDone)
	assert.NoError(t, err)

	tlMgr.taskAckManager.SetReadLevel(readLevel)
	tasks, readLevel, isReadBatchDone, err = tlMgr.taskReader.getTaskBatch()
	assert.Empty(t, tasks)
	assert.Equal(t, maxReadLevel, readLevel)
	assert.True(t, isReadBatchDone)
	assert.NoError(t, err)
}

func awaitCondition(cond func() bool, timeout time.Duration) bool {
	expiry := time.Now().Add(timeout)
	for !cond() {
		time.Sleep(time.Millisecond * 5)
		if time.Now().After(expiry) {
			return false
		}
	}
	return true
}

func TestTaskExpiryAndCompletion(t *testing.T) {
	const taskCount = 20
	const rangeSize = 10

	testCases := []struct {
		name               string
		batchSize          int
		maxTimeBtwnDeletes time.Duration
	}{
		{"test taskGC deleting due to size threshold", 2, time.Minute},
		{"test taskGC deleting due to time condition", 100, time.Nanosecond},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			maxTimeBetweenTaskDeletes = tc.maxTimeBtwnDeletes
			controller := gomock.NewController(t)
			mockPartitioner := partition.NewMockPartitioner(controller)
			mockPartitioner.EXPECT().GetIsolationGroupByDomainID(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
			mockDomainCache := cache.NewMockDomainCache(controller)
			mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(cache.CreateDomainCacheEntry("domainName"), nil).AnyTimes()
			mockDomainCache.EXPECT().GetDomainName(gomock.Any()).Return("domainName", nil).AnyTimes()
			logger := testlogger.New(t)
			tm := NewTestTaskManager(t, logger)
			taskListID := NewTestTaskListID(t, "domainId", "tl", 0)
			timeSource := clock.NewRealTimeSource()
			cfg := defaultTestConfig()
			cfg.RangeSize = rangeSize
			cfg.MaxTaskDeleteBatchSize = dynamicconfig.GetIntPropertyFilteredByTaskListInfo(tc.batchSize)
			// set idle timer check to a really small value to assert that we don't accidentally drop tasks while blocking
			// on enqueuing a task to task buffer
			cfg.IdleTasklistCheckInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskListInfo(10 * time.Millisecond)
			tlMgr, err := NewManager(
				mockDomainCache,
				logger,
				metrics.NewClient(tally.NoopScope, metrics.Matching),
				tm,
				cluster.GetTestClusterMetadata(true),
				mockPartitioner,
				nil,
				func(Manager) {},
				taskListID,
				types.TaskListKindNormal.Ptr(),
				cfg,
				timeSource,
				timeSource.Now(),
			)
			assert.NoError(t, err)
			tlm := tlMgr.(*taskListManagerImpl)
			err = tlm.Start()
			assert.NoError(t, err)
			for i := int64(0); i < taskCount; i++ {
				scheduleID := i * 3
				addParams := AddTaskParams{
					Execution: &types.WorkflowExecution{
						WorkflowID: "workflow1",
						RunID:      "run1",
					},
					TaskInfo: &persistence.TaskInfo{
						DomainID:               "domainId",
						RunID:                  "run1",
						WorkflowID:             "workflow1",
						ScheduleID:             scheduleID,
						ScheduleToStartTimeout: 5,
					},
				}
				if i%2 == 0 {
					// simulates creating a task whose scheduledToStartTimeout is already expired
					addParams.TaskInfo.ScheduleToStartTimeout = -5
				}
				_, err = tlm.AddTask(context.Background(), addParams)
				assert.NoError(t, err)
			}
			assert.Equal(t, taskCount, tm.GetTaskCount(taskListID))

			// wait until all tasks are loaded by into in-memory buffers by task list manager
			// the buffer size should be one less than expected because dispatcher will dequeue the head
			assert.True(t, awaitCondition(func() bool {
				return len(tlm.taskReader.taskBuffers[defaultTaskBufferIsolationGroup]) >= (taskCount/2 - 1)
			}, time.Second))

			remaining := taskCount
			for i := 0; i < 2; i++ {
				// verify that (1) expired tasks are not returned in poll result (2) taskCleaner deletes tasks correctly
				for i := int64(0); i < taskCount/4; i++ {
					task, err := tlm.GetTask(context.Background(), nil)
					assert.NoError(t, err)
					assert.NotNil(t, task)
					task.Finish(nil)
				}
				remaining -= taskCount / 2
				// since every other task is expired, we expect half the tasks to be deleted
				// after poll consumed 1/4th of what is available
				assert.Equal(t, remaining, tm.GetTaskCount(taskListID))
			}
			tlm.Stop()
		})
	}
}
