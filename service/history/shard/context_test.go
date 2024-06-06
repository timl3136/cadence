// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
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

package shard

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/engine"
	"github.com/uber/cadence/service/history/events"
	"github.com/uber/cadence/service/history/resource"
)

const (
	testShardID                   = 123
	testRangeID                   = 1
	testTransferMaxReadLevel      = 10
	testMaxTransferSequenceNumber = 100
	testCluster                   = "test-cluster"
	testDomain                    = "test-domain"
	testDomainID                  = "test-domain-id"
	testWorkflowID                = "test-workflow-id"
)

type (
	contextTestSuite struct {
		suite.Suite
		*require.Assertions

		controller       *gomock.Controller
		mockResource     *resource.Test
		mockShardManager *mocks.ShardManager

		metricsClient metrics.Client
		logger        log.Logger

		context *contextImpl
	}
)

func TestContextSuite(t *testing.T) {
	s := new(contextTestSuite)
	suite.Run(t, s)
}

func (s *contextTestSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockResource = resource.NewTest(s.T(), s.controller, metrics.History)
	s.mockShardManager = s.mockResource.ShardMgr

	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.History)
	s.logger = testlogger.New(s.T())

	s.context = s.newContext()
}

func (s *contextTestSuite) newContext() *contextImpl {
	eventsCache := events.NewMockCache(s.controller)
	config := config.NewForTest()
	shardInfo := &persistence.ShardInfo{
		ShardID: testShardID,
		RangeID: testRangeID,
		// the following fields will be initialized
		// when acquiring the shard if they are nil
		ClusterTransferAckLevel: make(map[string]int64),
		ClusterTimerAckLevel:    make(map[string]time.Time),
		ClusterReplicationLevel: make(map[string]int64),
		TransferProcessingQueueStates: &types.ProcessingQueueStates{
			StatesByCluster: make(map[string][]*types.ProcessingQueueState),
		},
		CrossClusterProcessingQueueStates: &types.ProcessingQueueStates{
			StatesByCluster: make(map[string][]*types.ProcessingQueueState),
		},
		TimerProcessingQueueStates: &types.ProcessingQueueStates{
			StatesByCluster: make(map[string][]*types.ProcessingQueueState),
		},
	}
	context := &contextImpl{
		Resource:                  s.mockResource,
		shardID:                   shardInfo.ShardID,
		rangeID:                   shardInfo.RangeID,
		shardInfo:                 shardInfo,
		executionManager:          s.mockResource.ExecutionMgr,
		config:                    config,
		logger:                    s.logger,
		throttledLogger:           s.logger,
		transferSequenceNumber:    1,
		transferMaxReadLevel:      testTransferMaxReadLevel,
		maxTransferSequenceNumber: testMaxTransferSequenceNumber,
		timerMaxReadLevelMap:      make(map[string]time.Time),
		remoteClusterCurrentTime:  make(map[string]time.Time),
		transferFailoverLevels:    make(map[string]TransferFailoverLevel),
		timerFailoverLevels:       make(map[string]TimerFailoverLevel),
		eventsCache:               eventsCache,
	}

	s.Require().True(testMaxTransferSequenceNumber < (1<<context.config.RangeSizeBits), "bad config value")

	return context
}

func (s *contextTestSuite) TearDownTest() {
	s.controller.Finish()
}

// test various setters and getters
func (s *contextTestSuite) TestAccessorMethods() {
	s.Assert().EqualValues(testShardID, s.context.GetShardID())
	s.Assert().Equal(s.mockResource, s.context.GetService())
	s.Assert().Equal(s.mockResource.ExecutionMgr, s.context.GetExecutionManager())
	s.Assert().EqualValues(testTransferMaxReadLevel, s.context.GetTransferMaxReadLevel())
	s.Assert().Equal(s.logger, s.context.GetLogger())
	s.Assert().Equal(s.logger, s.context.GetThrottledLogger())

	mockEngine := engine.NewMockEngine(s.controller)
	s.context.SetEngine(mockEngine)
	s.Assert().Equal(mockEngine, s.context.GetEngine())
}

func (s *contextTestSuite) TestTransferAckLevel() {
	// validate default value returned
	s.context.shardInfo.TransferAckLevel = 5
	s.Assert().EqualValues(5, s.context.GetTransferAckLevel())

	// update and validate it's returned
	s.mockShardManager.On("UpdateShard", mock.Anything, mock.Anything).Once().Return(nil)
	s.context.UpdateTransferAckLevel(20)
	s.Assert().EqualValues(20, s.context.GetTransferAckLevel())
	s.Assert().Equal(0, s.context.shardInfo.StolenSinceRenew)
}

func (s *contextTestSuite) TestClusterTransferAckLevel() {
	// update and validate cluster transfer ack level
	s.mockShardManager.On("UpdateShard", mock.Anything, mock.Anything).Once().Return(nil)
	s.context.UpdateTransferClusterAckLevel(cluster.TestCurrentClusterName, 5)
	s.Assert().EqualValues(5, s.context.GetTransferClusterAckLevel(cluster.TestCurrentClusterName))
	s.Assert().Equal(0, s.context.shardInfo.StolenSinceRenew)

	// get cluster transfer ack level for non existing cluster
	s.context.shardInfo.TransferAckLevel = 10
	s.Assert().EqualValues(10, s.context.GetTransferClusterAckLevel("non-existing-cluster"))
}

func (s *contextTestSuite) TestTimerAckLevel() {
	// validate default value returned
	now := time.Now()
	s.context.shardInfo.TimerAckLevel = now
	s.Assert().Equal(now.UnixNano(), s.context.GetTimerAckLevel().UnixNano())

	// update and validate it's returned
	newTime := time.Now()
	s.mockShardManager.On("UpdateShard", mock.Anything, mock.Anything).Once().Return(nil)
	s.context.UpdateTimerAckLevel(newTime)
	s.Assert().EqualValues(newTime.UnixNano(), s.context.GetTimerAckLevel().UnixNano())
	s.Assert().Equal(0, s.context.shardInfo.StolenSinceRenew)
}

func (s *contextTestSuite) TestClusterTimerAckLevel() {
	// update and validate cluster timer ack level
	now := time.Now()
	s.mockShardManager.On("UpdateShard", mock.Anything, mock.Anything).Once().Return(nil)
	s.context.UpdateTimerClusterAckLevel(cluster.TestCurrentClusterName, now)
	s.Assert().EqualValues(now.UnixNano(), s.context.GetTimerClusterAckLevel(cluster.TestCurrentClusterName).UnixNano())
	s.Assert().Equal(0, s.context.shardInfo.StolenSinceRenew)

	// get cluster timer ack level for non existing cluster
	s.context.shardInfo.TimerAckLevel = now
	s.Assert().EqualValues(now.UnixNano(), s.context.GetTimerClusterAckLevel("non-existing-cluster").UnixNano())
}

func (s *contextTestSuite) TestUpdateTransferFailoverLevel() {
	failoverLevel1 := TransferFailoverLevel{
		StartTime:    time.Now(),
		MinLevel:     1,
		CurrentLevel: 10,
		MaxLevel:     100,
		DomainIDs:    map[string]struct{}{"testDomainID": {}},
	}
	failoverLevel2 := TransferFailoverLevel{
		StartTime:    time.Now(),
		MinLevel:     2,
		CurrentLevel: 20,
		MaxLevel:     200,
		DomainIDs:    map[string]struct{}{"testDomainID2": {}},
	}

	err := s.context.UpdateTransferFailoverLevel("id1", failoverLevel1)
	s.NoError(err)
	err = s.context.UpdateTransferFailoverLevel("id2", failoverLevel2)
	s.NoError(err)

	gotLevels := s.context.GetAllTransferFailoverLevels()
	s.Len(gotLevels, 2)
	assert.Equal(s.T(), failoverLevel1, gotLevels["id1"])
	assert.Equal(s.T(), failoverLevel2, gotLevels["id2"])

	err = s.context.DeleteTransferFailoverLevel("id1")
	s.NoError(err)
	gotLevels = s.context.GetAllTransferFailoverLevels()
	s.Len(gotLevels, 1)
	assert.Equal(s.T(), failoverLevel2, gotLevels["id2"])
}

func (s *contextTestSuite) TestUpdateTimerFailoverLevel() {
	t := time.Now()
	failoverLevel1 := TimerFailoverLevel{
		StartTime:    t,
		MinLevel:     t.Add(time.Minute),
		CurrentLevel: t.Add(time.Minute * 2),
		MaxLevel:     t.Add(time.Minute * 3),
		DomainIDs:    map[string]struct{}{"testDomainID": {}},
	}
	failoverLevel2 := TimerFailoverLevel{
		StartTime:    t,
		MinLevel:     t.Add(time.Minute * 2),
		CurrentLevel: t.Add(time.Minute * 4),
		MaxLevel:     t.Add(time.Minute * 6),
		DomainIDs:    map[string]struct{}{"testDomainID2": {}},
	}

	err := s.context.UpdateTimerFailoverLevel("id1", failoverLevel1)
	s.NoError(err)
	err = s.context.UpdateTimerFailoverLevel("id2", failoverLevel2)
	s.NoError(err)

	gotLevels := s.context.GetAllTimerFailoverLevels()
	s.Len(gotLevels, 2)
	assert.Equal(s.T(), failoverLevel1, gotLevels["id1"])
	assert.Equal(s.T(), failoverLevel2, gotLevels["id2"])

	err = s.context.DeleteTimerFailoverLevel("id1")
	s.NoError(err)
	gotLevels = s.context.GetAllTimerFailoverLevels()
	s.Len(gotLevels, 1)
	assert.Equal(s.T(), failoverLevel2, gotLevels["id2"])
}

func (s *contextTestSuite) TestDomainNotificationVersion() {
	// test initial value
	s.EqualValues(0, s.context.GetDomainNotificationVersion())

	// test updated value
	s.mockShardManager.On("UpdateShard", mock.Anything, mock.Anything).Once().Return(nil)
	err := s.context.UpdateDomainNotificationVersion(10)
	s.NoError(err)
	s.EqualValues(10, s.context.GetDomainNotificationVersion())
}

func (s *contextTestSuite) TestTimerMaxReadLevel() {
	// get current cluster's level
	gotLevel := s.context.UpdateTimerMaxReadLevel(cluster.TestCurrentClusterName)
	wantLevel := s.mockResource.TimeSource.Now().Add(s.context.config.TimerProcessorMaxTimeShift()).Truncate(time.Millisecond)
	s.Equal(wantLevel, gotLevel)
	s.Equal(wantLevel, s.context.GetTimerMaxReadLevel(cluster.TestCurrentClusterName))

	// get remote cluster's level
	remoteCluster := "remote-cluster"
	now := time.Now()
	s.context.SetCurrentTime(remoteCluster, now)
	gotLevel = s.context.UpdateTimerMaxReadLevel(remoteCluster)
	wantLevel = now.Add(s.context.config.TimerProcessorMaxTimeShift()).Truncate(time.Millisecond)
	s.Equal(wantLevel, gotLevel)
	s.Equal(wantLevel, s.context.GetTimerMaxReadLevel(remoteCluster))
}

func (s *contextTestSuite) TestGenerateTransferTaskID() {
	taskID, err := s.context.GenerateTransferTaskID()
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), taskID)

	taskID, err = s.context.GenerateTransferTaskID()
	s.Require().NoError(err)
	s.Assert().Equal(int64(2), taskID)
}

func (s *contextTestSuite) TestGenerateTransferTaskIDs() {
	expectedTaskIDs := []int64{1, 2, 3, 4}

	taskIDs, err := s.context.GenerateTransferTaskIDs(4)
	s.Require().NoError(err)
	s.Assert().Equal(expectedTaskIDs, taskIDs)
}

func (s *contextTestSuite) TestGenerateTransferTaskID_RenewsRange() {
	// we acquire task IDs until testMaxTransferSequenceNumber, then next generation should involve
	// renewing range
	_, err := s.context.GenerateTransferTaskIDs(testMaxTransferSequenceNumber - 1)
	s.Require().NoError(err)

	s.mockShardManager.On("UpdateShard", mock.Anything, mock.Anything).Once().Return(nil)

	taskID, err := s.context.GenerateTransferTaskID()
	s.Require().NoError(err)

	newRangeID := testRangeID + 1
	s.Assert().EqualValues(newRangeID, s.context.getRangeID(), "RangeID should be incremented when renewing range")
	expectedTransferTaskID := newRangeID << s.context.config.RangeSizeBits

	s.Assert().EqualValues(expectedTransferTaskID, taskID)
}

func (s *contextTestSuite) TestRenewRangeLockedRetriesExceeded() {
	someError := errors.New("some error")
	s.mockShardManager.On("UpdateShard", mock.Anything, mock.Anything).Return(someError)

	err := s.context.renewRangeLocked(false)
	s.Error(err)
}

func (s *contextTestSuite) TestUpdateClusterReplicationLevel_Succeeds() {
	lastTaskID := int64(123)

	s.mockShardManager.On("UpdateShard", mock.Anything, mock.Anything).Once().Return(nil)
	err := s.context.UpdateClusterReplicationLevel(testCluster, lastTaskID)
	s.Require().NoError(err)

	s.Equal(lastTaskID, s.context.GetClusterReplicationLevel(testCluster))
}

func (s *contextTestSuite) TestUpdateClusterReplicationLevel_FailsWhenUpdateShardFail() {
	ownershipLostError := &persistence.ShardOwnershipLostError{ShardID: testShardID, Msg: "testing ownership lost"}
	shardClosed := false
	closeCallbackCalled := make(chan bool)

	s.mockShardManager.On("UpdateShard", mock.Anything, mock.Anything).Return(ownershipLostError)
	s.context.closeCallback = func(int, *historyShardsItem) {
		shardClosed = true
		closeCallbackCalled <- true
	}

	err := s.context.UpdateClusterReplicationLevel(testCluster, int64(123))

	select {
	case <-closeCallbackCalled:
		break
	case <-time.NewTimer(time.Second).C:
		s.T().Fatal("close callback is still not called")
	}

	s.Require().ErrorContains(err, ownershipLostError.Msg)
	s.True(shardClosed, "the shard should have been closed on ShardOwnershipLostError")
}

func (s *contextTestSuite) TestReplicateFailoverMarkersSuccess() {
	s.mockResource.ExecutionMgr.On("CreateFailoverMarkerTasks", mock.Anything, mock.Anything).Once().Return(nil)

	markers := make([]*persistence.FailoverMarkerTask, 0)
	err := s.context.ReplicateFailoverMarkers(context.Background(), markers)
	s.NoError(err)
}

func (s *contextTestSuite) TestCreateWorkflowExecution_Succeeds() {
	ctx := context.Background()
	request := &persistence.CreateWorkflowExecutionRequest{
		DomainName: testDomain,
		NewWorkflowSnapshot: persistence.WorkflowSnapshot{
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				DomainID:   testDomainID,
				WorkflowID: testWorkflowID,
			},
		},
	}

	domainCacheEntry := cache.NewLocalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: testDomainID},
		&persistence.DomainConfig{Retention: 7},
		testCluster,
	)
	s.mockResource.DomainCache.EXPECT().GetDomainByID(testDomainID).Return(domainCacheEntry, nil)

	persistenceResponse := &persistence.CreateWorkflowExecutionResponse{}
	s.mockResource.ExecutionMgr.On("CreateWorkflowExecution", ctx, mock.Anything).Once().Return(persistenceResponse, nil)

	resp, err := s.context.CreateWorkflowExecution(ctx, request)
	s.NoError(err)
	s.NotNil(resp)
}

func (s *contextTestSuite) TestValidateAndUpdateFailoverMarkers() {
	domainFailoverVersion := 100
	domainCacheEntryInactiveCluster := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: testDomainID},
		&persistence.DomainConfig{Retention: 7},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName, // active is TestCurrentClusterName
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		int64(domainFailoverVersion),
	)
	domainCacheEntryActiveCluster := cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: testDomainID},
		&persistence.DomainConfig{Retention: 7},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName, // active cluster
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		int64(domainFailoverVersion),
	)
	s.mockResource.DomainCache.EXPECT().GetDomainByID(testDomainID).Return(domainCacheEntryInactiveCluster, nil)

	failoverMarker := types.FailoverMarkerAttributes{
		DomainID:        testDomainID,
		FailoverVersion: 101,
	}

	s.mockShardManager.On("UpdateShard", mock.Anything, mock.Anything).Return(nil)
	s.NoError(s.context.AddingPendingFailoverMarker(&failoverMarker))
	s.Require().Len(s.context.shardInfo.PendingFailoverMarkers, 1, "we should have one failover marker saved since the cluster is not active")

	s.mockResource.DomainCache.EXPECT().GetDomainByID(testDomainID).Return(domainCacheEntryActiveCluster, nil)

	pendingFailoverMarkers, err := s.context.ValidateAndUpdateFailoverMarkers()
	s.NoError(err)
	s.Empty(pendingFailoverMarkers, "all pending failover tasks should be cleaned up")
}

func (s *contextTestSuite) TestGetAndUpdateProcessingQueueStates() {
	clusterName := cluster.TestCurrentClusterName
	var initialQueueStates [][]*types.ProcessingQueueState
	initialQueueStates = append(initialQueueStates, s.context.GetTransferProcessingQueueStates(clusterName))
	initialQueueStates = append(initialQueueStates, s.context.GetCrossClusterProcessingQueueStates(clusterName))
	initialQueueStates = append(initialQueueStates, s.context.GetTimerProcessingQueueStates(clusterName))
	for _, queueStates := range initialQueueStates {
		s.Len(queueStates, 1)
		s.Zero(queueStates[0].GetLevel())
		ackLevel := queueStates[0].GetAckLevel()
		if ackLevel != 0 {
			// for timer queue
			s.Equal(time.Time{}.UnixNano(), ackLevel)
		}
		s.Equal(int64(math.MaxInt64), queueStates[0].GetMaxLevel())
		s.Equal(&types.DomainFilter{ReverseMatch: true}, queueStates[0].GetDomainFilter())
	}

	s.mockShardManager.On("UpdateShard", mock.Anything, mock.Anything).Return(nil)
	updatedTransferQueueStates := []*types.ProcessingQueueState{
		{
			Level:    common.Int32Ptr(0),
			AckLevel: common.Int64Ptr(123),
			MaxLevel: common.Int64Ptr(math.MaxInt64),
			DomainFilter: &types.DomainFilter{
				ReverseMatch: true,
			},
		},
	}
	updatedCrossClusterQueueStates := []*types.ProcessingQueueState{
		{
			Level:    common.Int32Ptr(0),
			AckLevel: common.Int64Ptr(456),
			MaxLevel: common.Int64Ptr(math.MaxInt64),
			DomainFilter: &types.DomainFilter{
				ReverseMatch: true,
				DomainIDs:    []string{"testDomainID"},
			},
		},
		{
			Level:    common.Int32Ptr(1),
			AckLevel: common.Int64Ptr(123),
			MaxLevel: common.Int64Ptr(math.MaxInt64),
			DomainFilter: &types.DomainFilter{
				ReverseMatch: false,
				DomainIDs:    []string{"testDomainID"},
			},
		},
	}
	updatedTimerQueueStates := []*types.ProcessingQueueState{
		{
			Level:    common.Int32Ptr(0),
			AckLevel: common.Int64Ptr(time.Now().UnixNano()),
			MaxLevel: common.Int64Ptr(math.MaxInt64),
			DomainFilter: &types.DomainFilter{
				ReverseMatch: true,
			},
		},
	}
	err := s.context.UpdateTransferProcessingQueueStates(clusterName, updatedTransferQueueStates)
	s.NoError(err)
	err = s.context.UpdateCrossClusterProcessingQueueStates(clusterName, updatedCrossClusterQueueStates)
	s.NoError(err)
	err = s.context.UpdateTimerProcessingQueueStates(clusterName, updatedTimerQueueStates)
	s.NoError(err)

	s.Equal(updatedTransferQueueStates, s.context.GetTransferProcessingQueueStates(clusterName))
	s.Equal(updatedCrossClusterQueueStates, s.context.GetCrossClusterProcessingQueueStates(clusterName))
	s.Equal(updatedTimerQueueStates, s.context.GetTimerProcessingQueueStates(clusterName))

	// check if cluster ack level for transfer and timer is backfilled for backward compatibility
	s.Equal(updatedTransferQueueStates[0].GetAckLevel(), s.context.GetTransferClusterAckLevel(clusterName))
	s.Equal(time.Unix(0, updatedTimerQueueStates[0].GetAckLevel()), s.context.GetTimerClusterAckLevel(clusterName))
}

func TestGetWorkflowExecution(t *testing.T) {
	testCases := []struct {
		name           string
		request        *persistence.GetWorkflowExecutionRequest
		mockSetup      func(*mocks.ExecutionManager)
		expectedResult *persistence.GetWorkflowExecutionResponse
		expectedError  error
	}{
		{
			name: "Success",
			request: &persistence.GetWorkflowExecutionRequest{
				DomainID:  "testDomain",
				Execution: types.WorkflowExecution{WorkflowID: "testWorkflowID", RunID: "testRunID"},
			},
			mockSetup: func(mgr *mocks.ExecutionManager) {
				mgr.On("GetWorkflowExecution", mock.Anything, mock.Anything).Return(&persistence.GetWorkflowExecutionResponse{
					State: &persistence.WorkflowMutableState{
						ExecutionInfo: &persistence.WorkflowExecutionInfo{
							DomainID:   "testDomain",
							WorkflowID: "testWorkflowID",
							RunID:      "testRunID",
						},
					},
				}, nil)
			},
			expectedResult: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						DomainID:   "testDomain",
						WorkflowID: "testWorkflowID",
						RunID:      "testRunID",
					},
				},
			},
			expectedError: nil,
		},
		{
			name: "Error",
			request: &persistence.GetWorkflowExecutionRequest{
				DomainID:  "testDomain",
				Execution: types.WorkflowExecution{WorkflowID: "testWorkflowID", RunID: "testRunID"},
			},
			mockSetup: func(mgr *mocks.ExecutionManager) {
				mgr.On("GetWorkflowExecution", mock.Anything, mock.Anything).Return(nil, errors.New("some random error"))
			},
			expectedResult: nil,
			expectedError:  errors.New("some random error"),
		},
	}

	for _, tc := range testCases {
		mockExecutionMgr := &mocks.ExecutionManager{}
		shardContext := &contextImpl{
			executionManager: mockExecutionMgr,
			shardInfo: &persistence.ShardInfo{
				RangeID: 12,
			},
		}
		tc.mockSetup(mockExecutionMgr)

		result, err := shardContext.GetWorkflowExecution(context.Background(), tc.request)
		assert.Equal(t, tc.expectedResult, result)
		assert.Equal(t, tc.expectedError, err)
	}
}

func TestCloseShard(t *testing.T) {
	closeCallback := make(chan struct{})

	shardContext := &contextImpl{
		shardInfo: &persistence.ShardInfo{RangeID: 12},
		closeCallback: func(i int, item *historyShardsItem) {
			close(closeCallback)
		},
	}
	shardContext.closeShard()

	select {
	case <-closeCallback:
	case <-time.After(time.Second):
		assert.Fail(t, "closeCallback not called")
	}

	assert.WithinDuration(t, time.Now(), *shardContext.closedAt.Load(), time.Second)
	assert.Equal(t, int64(-1), shardContext.shardInfo.RangeID)
}

func TestCloseShard_AlreadyClosed(t *testing.T) {
	closeTime := time.Unix(123, 456)

	shardContext := &contextImpl{
		closeCallback: func(i int, item *historyShardsItem) {
			assert.Fail(t, "closeCallback should not be called")
		},
	}
	shardContext.closedAt.Store(&closeTime)
	shardContext.closeShard()
	assert.Equal(t, closeTime, *shardContext.closedAt.Load())
}

func TestShardClosedGuard(t *testing.T) {
	shardContext := &contextImpl{
		shardInfo: &persistence.ShardInfo{},
	}

	testCases := []struct {
		name string
		call func() error
	}{
		{
			name: "GetWorkflowExecution",
			call: func() error {
				_, err := shardContext.GetWorkflowExecution(
					context.Background(),
					&persistence.GetWorkflowExecutionRequest{RangeID: 0},
				)
				return err
			},
		},
		{
			name: "CreateWorkflowExecution",
			call: func() error {
				_, err := shardContext.CreateWorkflowExecution(context.Background(), nil)
				return err
			},
		},
		{
			name: "UpdateWorkflowExecution",
			call: func() error {
				_, err := shardContext.UpdateWorkflowExecution(context.Background(), nil)
				return err
			},
		},
		{
			name: "ConflictResolveWorkflowExecution",
			call: func() error {
				_, err := shardContext.ConflictResolveWorkflowExecution(context.Background(), nil)
				return err
			},
		},
		{
			name: "AppendHistoryV2Events",
			call: func() error {
				_, err := shardContext.AppendHistoryV2Events(context.Background(), nil, "", types.WorkflowExecution{})
				return err
			},
		},
		{
			name: "renewRangeLocked",
			call: func() error {
				return shardContext.renewRangeLocked(false)
			},
		},
		{
			name: "persistShardInfoLocked",
			call: func() error {
				return shardContext.persistShardInfoLocked(false)
			},
		},
		{
			name: "ReplicateFailoverMarkers",
			call: func() error {
				return shardContext.ReplicateFailoverMarkers(context.Background(), nil)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			closedAt := time.Unix(123, 456)

			shardContext.closedAt.Store(&closedAt)
			err := tc.call()
			var shardClosedErr *ErrShardClosed
			assert.ErrorAs(t, err, &shardClosedErr)
			assert.Equal(t, closedAt, shardClosedErr.ClosedAt)
			assert.ErrorContains(t, err, "shard closed")
		})
	}
}
