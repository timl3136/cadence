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

package persistence

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/checksum"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/types"
)

var (
	testIndex             = "test-index"
	testDomain            = "test-domain"
	testDomainID          = "bfd5c907-f899-4baf-a7b2-2ab85e623ebd"
	testPageSize          = 10
	testEarliestTime      = int64(1547596872371000000)
	testLatestTime        = int64(2547596872371000000)
	testWorkflowType      = "test-wf-type"
	testWorkflowID        = "test-wid"
	testCloseStatus       = int32(1)
	testTableName         = "test-table-name"
	testRunID             = "test-run-id"
	testSearchAttributes1 = map[string]interface{}{"TestAttr1": "val1", "TestAttr2": 2, "TestAttr3": false}
	testSearchAttributes2 = map[string]interface{}{"TestAttr1": "val2", "TestAttr2": 2, "TestAttr3": false}
	testSearchAttributes3 = map[string]interface{}{"TestAttr2": 2, "TestAttr3": false}
)

func TestExecutionManager_ProxyStoreMethods(t *testing.T) {
	for _, tc := range []struct {
		method       string
		prepareMocks func(*MockExecutionStore)
	}{
		{
			method: "GetShardID",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().GetShardID().Return(1).Times(1)
			},
		},
		{
			method: "GetName",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().GetName().Return("test").Times(1)
			},
		},
		{
			method: "Close",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().Close().Return().Times(1)
			},
		},
		{
			method: "GetTransferTasks",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().GetTransferTasks(gomock.Any(), gomock.Any()).Return(nil, nil)
			},
		},
		{
			method: "CompleteTransferTask",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().CompleteTransferTask(gomock.Any(), gomock.Any()).Return(nil)
			},
		},
		{
			method: "RangeCompleteTransferTask",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().RangeCompleteTransferTask(gomock.Any(), gomock.Any()).Return(nil, nil)
			},
		},
		{
			method: "GetCrossClusterTasks",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().GetCrossClusterTasks(gomock.Any(), gomock.Any()).Return(nil, nil)
			},
		},
		{
			method: "CompleteCrossClusterTask",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().CompleteCrossClusterTask(gomock.Any(), gomock.Any()).Return(nil)
			},
		},
		{
			method: "RangeCompleteCrossClusterTask",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().RangeCompleteCrossClusterTask(gomock.Any(), gomock.Any()).Return(nil, nil)
			},
		},
		{
			method: "DeleteReplicationTaskFromDLQ",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().DeleteReplicationTaskFromDLQ(gomock.Any(), gomock.Any()).Return(nil)
			},
		},
		{
			method: "RangeDeleteReplicationTaskFromDLQ",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().RangeDeleteReplicationTaskFromDLQ(gomock.Any(), gomock.Any()).Return(nil, nil)
			},
		},
		{
			method: "CreateFailoverMarkerTasks",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().CreateFailoverMarkerTasks(gomock.Any(), gomock.Any()).Return(nil)
			},
		},
		{
			method: "GetTimerIndexTasks",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().GetTimerIndexTasks(gomock.Any(), gomock.Any()).Return(nil, nil)
			},
		},
		{
			method: "CompleteTimerTask",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().CompleteTimerTask(gomock.Any(), gomock.Any()).Return(nil)
			},
		},
		{
			method: "RangeCompleteTimerTask",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().RangeCompleteTimerTask(gomock.Any(), gomock.Any()).Return(nil, nil)
			},
		},
		{
			method: "CompleteReplicationTask",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().CompleteReplicationTask(gomock.Any(), gomock.Any()).Return(nil)
			},
		},
		{
			method: "RangeCompleteReplicationTask",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().RangeCompleteReplicationTask(gomock.Any(), gomock.Any()).Return(nil, nil)
			},
		},
		{
			method: "DeleteWorkflowExecution",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
			},
		},
		{
			method: "DeleteCurrentWorkflowExecution",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
			},
		},
		{
			method: "GetCurrentExecution",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, nil)
			},
		},
		{
			method: "ListCurrentExecutions",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().ListCurrentExecutions(gomock.Any(), gomock.Any()).Return(nil, nil)
			},
		},
		{
			method: "IsWorkflowExecutionExists",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().IsWorkflowExecutionExists(gomock.Any(), gomock.Any()).Return(nil, nil)
			},
		},
		{
			method: "GetReplicationDLQSize",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().GetReplicationDLQSize(gomock.Any(), gomock.Any()).Return(nil, nil)
			},
		},
	} {
		t.Run(tc.method, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockedStore := NewMockExecutionStore(ctrl)
			tc.prepareMocks(mockedStore)
			manager := NewExecutionManagerImpl(mockedStore, testlogger.New(t), nil)
			v := reflect.ValueOf(manager)
			method := v.MethodByName(tc.method)
			methodType := method.Type()
			args := methodType.NumIn()
			var vals []reflect.Value
			// If a method requires arguments, we expect the first argument to be a context
			// and the rest to be zero values of the correct type.
			// For methods like Close and GetShardID, we don't expect any arguments.
			if args > 0 {
				vals = append(vals, reflect.ValueOf(context.Background()))
				for i := 1; i < args; i++ {
					vals = append(vals, reflect.Zero(methodType.In(i)))
				}
			}

			callRes := method.Call(vals)
			if callRes == nil {
				return
			}
			resultErr := callRes[len(callRes)-1].Interface()
			err, ok := resultErr.(error)
			if ok {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetReplicationTasks(t *testing.T) {
	for _, tc := range []struct {
		name         string
		prepareMocks func(*MockExecutionStore)
		checkRes     func(*testing.T, *GetReplicationTasksResponse, error)
	}{
		{
			name: "success",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().GetReplicationTasks(gomock.Any(), gomock.Any()).Return(&InternalGetReplicationTasksResponse{
					Tasks: []*InternalReplicationTaskInfo{
						{
							DomainID: "test",
							TaskID:   1,
						},
						{
							DomainID: "test",
							TaskID:   2,
						},
					},
					NextPageToken: nil,
				}, nil)
			},
			checkRes: func(t *testing.T, res *GetReplicationTasksResponse, err error) {
				assert.NoError(t, err)
				assert.Len(t, res.Tasks, 2)
				assert.Equal(t, int64(1), res.Tasks[0].TaskID)
				assert.Equal(t, int64(2), res.Tasks[1].TaskID)
				assert.Nil(t, res.NextPageToken)
			},
		},
		{
			name: "error",
			prepareMocks: func(mockedStore *MockExecutionStore) {
				mockedStore.EXPECT().GetReplicationTasks(gomock.Any(), gomock.Any()).Return(nil, assert.AnError)
			},
			checkRes: func(t *testing.T, res *GetReplicationTasksResponse, err error) {
				assert.Error(t, err)
				assert.Nil(t, res)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			mockedStore := NewMockExecutionStore(ctrl)
			tc.prepareMocks(mockedStore)
			manager := NewExecutionManagerImpl(mockedStore, testlogger.New(t), nil)
			res, err := manager.GetReplicationTasks(context.Background(), &GetReplicationTasksRequest{})
			tc.checkRes(t, res, err)
		})
	}
}

func TestExecutionManager_GetWorkflowExecution(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockedStore := NewMockExecutionStore(ctrl)
	mockedSerializer := NewMockPayloadSerializer(ctrl)

	manager := NewExecutionManagerImpl(mockedStore, testlogger.New(t), mockedSerializer)

	request := &GetWorkflowExecutionRequest{
		DomainID: testDomainID,
		Execution: types.WorkflowExecution{
			WorkflowID: testWorkflowID,
			RunID:      testRunID,
		},
		RangeID: 1,
	}

	activityOne := sampleInternalActivityInfo("activity1")
	activityTwo := sampleInternalActivityInfo("activity2")

	wfCompletionEvent := NewDataBlob([]byte("wf-event"), common.EncodingTypeThriftRW)
	wfCompletionEventData := generateTestHistoryEvent(99)

	wfInfo := sampleInternalWorkflowExecutionInfo()
	wfInfo.CompletionEvent = wfCompletionEvent
	wfInfo.AutoResetPoints = NewDataBlob([]byte("test-reset-points"), common.EncodingTypeThriftRW)

	mockedStore.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&InternalGetWorkflowExecutionResponse{
		State: &InternalWorkflowMutableState{
			ExecutionInfo: wfInfo,
			ActivityInfos: map[int64]*InternalActivityInfo{
				1: activityOne,
				2: activityTwo,
			},
			TimerInfos: map[string]*TimerInfo{
				"test-timer": {
					Version: 1,
				},
			},
		},
	}, nil)

	mockedSerializer.EXPECT().DeserializeEvent(activityOne.ScheduledEvent).Return(&types.HistoryEvent{
		ID: 1,
	}, nil).Times(1)
	mockedSerializer.EXPECT().DeserializeEvent(activityOne.StartedEvent).Return(&types.HistoryEvent{
		ID: 1,
	}, nil).Times(1)

	mockedSerializer.EXPECT().DeserializeEvent(activityTwo.ScheduledEvent).Return(&types.HistoryEvent{
		ID: 2,
	}, nil).Times(1)
	mockedSerializer.EXPECT().DeserializeEvent(activityTwo.StartedEvent).Return(&types.HistoryEvent{
		ID: 2,
	}, nil).Times(1)

	mockedSerializer.EXPECT().DeserializeEvent(wfCompletionEvent).Return(wfCompletionEventData, nil).Times(1)
	mockedSerializer.EXPECT().DeserializeResetPoints(gomock.Any()).Return(&types.ResetPoints{}, nil).Times(1)
	mockedSerializer.EXPECT().DeserializeChecksum(gomock.Any()).Return(checksum.Checksum{}, nil).Times(1)

	res, err := manager.GetWorkflowExecution(context.Background(), request)
	assert.NoError(t, err)

	expectedExecutionInfo := sampleWorkflowExecutionInfo()
	expectedExecutionInfo.CompletionEvent = wfCompletionEventData
	expectedExecutionInfo.AutoResetPoints = &types.ResetPoints{}

	assert.Equal(t, &WorkflowMutableState{
		ExecutionInfo:       expectedExecutionInfo,
		ChildExecutionInfos: make(map[int64]*ChildExecutionInfo),
		ActivityInfos: map[int64]*ActivityInfo{
			1: sampleActivityInfo("activity1", 1),
			2: sampleActivityInfo("activity2", 2),
		},
		TimerInfos: map[string]*TimerInfo{
			"test-timer": {
				Version: 1,
			},
		},
		ExecutionStats: &ExecutionStats{
			HistorySize: 1024,
		},
		BufferedEvents: make([]*types.HistoryEvent, 0),
	}, res.State)
	// Expectations for the deserialization of activity events.
	assert.Equal(t, &MutableStateStats{MutableStateSize: 170, ExecutionInfoSize: 20, ActivityInfoSize: 150, TimerInfoSize: 0, ChildInfoSize: 0, SignalInfoSize: 0, BufferedEventsSize: 0, ActivityInfoCount: 2, TimerInfoCount: 1, ChildInfoCount: 0, SignalInfoCount: 0, RequestCancelInfoCount: 0, BufferedEventsCount: 0}, res.MutableStateStats)
}

func TestExecutionManager_GetWorkflowExecution_NoWorkflow(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockedStore := NewMockExecutionStore(ctrl)
	mockedSerializer := NewMockPayloadSerializer(ctrl)

	manager := NewExecutionManagerImpl(mockedStore, testlogger.New(t), mockedSerializer)

	request := &GetWorkflowExecutionRequest{
		DomainID: "testDomain",
		Execution: types.WorkflowExecution{
			WorkflowID: "nonexistentWorkflow",
			RunID:      "nonexistentRunID",
		},
		RangeID: 1,
	}

	mockedStore.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, &types.EntityNotExistsError{})

	_, err := manager.GetWorkflowExecution(context.Background(), request)
	assert.Error(t, err)
	assert.IsType(t, &types.EntityNotExistsError{}, err)
}

func TestExecutionManager_UpdateWorkflowExecution(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockedStore := NewMockExecutionStore(ctrl)
	mockedSerializer := NewMockPayloadSerializer(ctrl)

	manager := NewExecutionManagerImpl(mockedStore, testlogger.New(t), mockedSerializer)

	info := sampleWorkflowExecutionInfo()
	info.CompletionEvent = &types.HistoryEvent{
		ID:        1,
		EventType: types.EventTypeWorkflowExecutionCompleted.Ptr(),
	}
	info.AutoResetPoints = &types.ResetPoints{
		Points: []*types.ResetPointInfo{
			{
				BinaryChecksum: "test-checksum",
			},
		},
	}

	expectedInfo := sampleInternalWorkflowMutation()
	expectedInfo.ExecutionInfo.AutoResetPoints = &DataBlob{
		Encoding: common.EncodingTypeThriftRW,
		Data:     []byte("test-reset-points"),
	}
	expectedInfo.ExecutionInfo.CompletionEvent = &DataBlob{
		Encoding: common.EncodingTypeThriftRW,
		Data:     []byte("test-event"),
	}
	expectedInfo.ChecksumData = sampleCheckSumData()
	expectedInfo.StartVersion = common.EmptyVersion
	expectedInfo.LastWriteVersion = common.EmptyVersion
	expectedInfo.UpsertActivityInfos = []*InternalActivityInfo{}
	expectedInfo.UpsertChildExecutionInfos = []*InternalChildExecutionInfo{}

	mockedSerializer.EXPECT().SerializeEvent(info.CompletionEvent, common.EncodingTypeThriftRW).Return(expectedInfo.ExecutionInfo.CompletionEvent, nil).Times(2)
	mockedSerializer.EXPECT().SerializeResetPoints(info.AutoResetPoints, common.EncodingTypeThriftRW).Return(expectedInfo.ExecutionInfo.AutoResetPoints, nil).Times(2)

	request := &UpdateWorkflowExecutionRequest{
		RangeID: 1,
		Mode:    UpdateWorkflowModeBypassCurrent,
		UpdateWorkflowMutation: WorkflowMutation{
			ExecutionInfo:  info,
			ExecutionStats: &ExecutionStats{},
			Checksum:       generateChecksum(),
		},
		Encoding: common.EncodingTypeThriftRW,
		NewWorkflowSnapshot: &WorkflowSnapshot{
			ExecutionInfo: info,
			ExecutionStats: &ExecutionStats{
				HistorySize: 1024,
			},
			Checksum: generateChecksum(),
		},
	}

	expectedInfo.Checksum = request.UpdateWorkflowMutation.Checksum
	mockedSerializer.EXPECT().SerializeChecksum(request.UpdateWorkflowMutation.Checksum, common.EncodingTypeJSON).Return(expectedInfo.ChecksumData, nil).Times(2)

	expectedRequest := &InternalUpdateWorkflowExecutionRequest{
		RangeID:                1,
		Mode:                   UpdateWorkflowModeBypassCurrent,
		UpdateWorkflowMutation: *expectedInfo,
		NewWorkflowSnapshot: &InternalWorkflowSnapshot{
			ExecutionInfo: expectedInfo.ExecutionInfo,
		},
	}
	mockedStore.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, req *InternalUpdateWorkflowExecutionRequest) error {
		assert.Equal(t, expectedRequest.UpdateWorkflowMutation, req.UpdateWorkflowMutation)
		return nil
	})

	res, err := manager.UpdateWorkflowExecution(context.Background(), request)
	assert.NoError(t, err)
	stats := &MutableStateUpdateSessionStats{
		MutableStateSize:  40,
		ExecutionInfoSize: 40,
	}
	assert.Equal(t, stats, res.MutableStateUpdateSessionStats)
}

func TestSerializeWorkflowSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name         string
		prepareMocks func(*MockPayloadSerializer)
		input        *WorkflowSnapshot
		checkRes     func(*testing.T, *InternalWorkflowSnapshot, error)
	}{
		{
			name: "success",
			prepareMocks: func(mockedSerializer *MockPayloadSerializer) {
				mockedSerializer.EXPECT().SerializeEvent(completionEvent(), common.EncodingTypeThriftRW).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeResetPoints(gomock.Any(), gomock.Any()).Return(sampleResetPointsData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeVersionHistories(gomock.Any(), gomock.Any()).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeEvent(activityScheduledEvent(), common.EncodingTypeThriftRW).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeEvent(activityStartedEvent(), common.EncodingTypeThriftRW).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeEvent(childWorkflowScheduledEvent(), common.EncodingTypeThriftRW).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeEvent(childWorkflowStartedEvent(), common.EncodingTypeThriftRW).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeChecksum(gomock.Any(), gomock.Any()).Return(sampleCheckSumData(), nil).Times(1)
			},
			input: sampleWorkflowSnapshot(),
			checkRes: func(t *testing.T, res *InternalWorkflowSnapshot, err error) {
				assert.NoError(t, err)
				assert.Equal(t, sampleInternalWorkflowSnapshot(), res)
			},
		},
		{
			name: "nil info",
			prepareMocks: func(mockedSerializer *MockPayloadSerializer) {
				mockedSerializer.EXPECT().SerializeChecksum(gomock.Any(), gomock.Any()).Return(sampleTestCheckSumData(), nil).Times(1)
			},
			input: &WorkflowSnapshot{},
			checkRes: func(t *testing.T, res *InternalWorkflowSnapshot, err error) {
				assert.NoError(t, err)
				assert.Equal(t, &InternalWorkflowSnapshot{
					ExecutionInfo:       &InternalWorkflowExecutionInfo{},
					ChecksumData:        sampleTestCheckSumData(),
					StartVersion:        common.EmptyVersion,
					LastWriteVersion:    common.EmptyVersion,
					ActivityInfos:       make([]*InternalActivityInfo, 0),
					ChildExecutionInfos: make([]*InternalChildExecutionInfo, 0),
				}, res)
			},
		},
		{
			name: "serialize event error",
			prepareMocks: func(mockedSerializer *MockPayloadSerializer) {
				mockedSerializer.EXPECT().SerializeEvent(gomock.Any(), gomock.Any()).Return(nil, assert.AnError).Times(1)
			},
			input: sampleWorkflowSnapshot(),
			checkRes: func(t *testing.T, res *InternalWorkflowSnapshot, err error) {
				assert.ErrorIs(t, err, assert.AnError)
			},
		},
		{
			name: "serialize points error",
			prepareMocks: func(mockedSerializer *MockPayloadSerializer) {
				mockedSerializer.EXPECT().SerializeEvent(gomock.Any(), gomock.Any()).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeResetPoints(gomock.Any(), gomock.Any()).Return(nil, assert.AnError).Times(1)
			},
			input: sampleWorkflowSnapshot(),
			checkRes: func(t *testing.T, res *InternalWorkflowSnapshot, err error) {
				assert.ErrorIs(t, err, assert.AnError)
			},
		},
		{
			name: "serialize version histories error",
			prepareMocks: func(mockedSerializer *MockPayloadSerializer) {
				mockedSerializer.EXPECT().SerializeEvent(gomock.Any(), gomock.Any()).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeResetPoints(gomock.Any(), gomock.Any()).Return(NewDataBlob([]byte("test-reset-points"), common.EncodingTypeThriftRW), nil).Times(1)
				mockedSerializer.EXPECT().SerializeVersionHistories(gomock.Any(), gomock.Any()).Return(nil, assert.AnError).Times(1)
			},
			input: sampleWorkflowSnapshot(),
			checkRes: func(t *testing.T, res *InternalWorkflowSnapshot, err error) {
				assert.ErrorIs(t, err, assert.AnError)
			},
		},
		{
			name: "serialize activity scheduled event error",
			prepareMocks: func(mockedSerializer *MockPayloadSerializer) {
				mockedSerializer.EXPECT().SerializeEvent(completionEvent(), common.EncodingTypeThriftRW).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeResetPoints(gomock.Any(), gomock.Any()).Return(sampleResetPointsData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeVersionHistories(gomock.Any(), gomock.Any()).Return(sampleTestCheckSumData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeEvent(activityScheduledEvent(), common.EncodingTypeThriftRW).Return(nil, assert.AnError).Times(1)
			},
			input: sampleWorkflowSnapshot(),
			checkRes: func(t *testing.T, res *InternalWorkflowSnapshot, err error) {
				assert.ErrorIs(t, err, assert.AnError)
			},
		},
		{
			name: "serialize activity started event error",
			prepareMocks: func(mockedSerializer *MockPayloadSerializer) {
				mockedSerializer.EXPECT().SerializeEvent(completionEvent(), common.EncodingTypeThriftRW).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeResetPoints(gomock.Any(), gomock.Any()).Return(sampleResetPointsData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeVersionHistories(gomock.Any(), gomock.Any()).Return(sampleTestCheckSumData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeEvent(activityScheduledEvent(), common.EncodingTypeThriftRW).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeEvent(activityStartedEvent(), common.EncodingTypeThriftRW).Return(nil, assert.AnError).Times(1)
			},
			input: sampleWorkflowSnapshot(),
			checkRes: func(t *testing.T, res *InternalWorkflowSnapshot, err error) {
				assert.ErrorIs(t, err, assert.AnError)
			},
		},

		{
			name: "serialize child workflow scheduled event error",
			prepareMocks: func(mockedSerializer *MockPayloadSerializer) {
				mockedSerializer.EXPECT().SerializeEvent(completionEvent(), common.EncodingTypeThriftRW).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeResetPoints(gomock.Any(), gomock.Any()).Return(sampleResetPointsData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeVersionHistories(gomock.Any(), gomock.Any()).Return(sampleTestCheckSumData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeEvent(activityScheduledEvent(), common.EncodingTypeThriftRW).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeEvent(activityStartedEvent(), common.EncodingTypeThriftRW).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeEvent(childWorkflowScheduledEvent(), common.EncodingTypeThriftRW).Return(nil, assert.AnError).Times(1)
			},
			input: sampleWorkflowSnapshot(),
			checkRes: func(t *testing.T, res *InternalWorkflowSnapshot, err error) {
				assert.ErrorIs(t, err, assert.AnError)
			},
		},
		{
			name: "serialize child workflow started event error",
			prepareMocks: func(mockedSerializer *MockPayloadSerializer) {
				mockedSerializer.EXPECT().SerializeEvent(completionEvent(), common.EncodingTypeThriftRW).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeResetPoints(gomock.Any(), gomock.Any()).Return(sampleResetPointsData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeVersionHistories(gomock.Any(), gomock.Any()).Return(sampleTestCheckSumData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeEvent(activityScheduledEvent(), common.EncodingTypeThriftRW).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeEvent(activityStartedEvent(), common.EncodingTypeThriftRW).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeEvent(childWorkflowScheduledEvent(), common.EncodingTypeThriftRW).Return(sampleEventData(), nil).Times(1)
				mockedSerializer.EXPECT().SerializeEvent(childWorkflowStartedEvent(), common.EncodingTypeThriftRW).Return(nil, assert.AnError).Times(1)
			},
			input: sampleWorkflowSnapshot(),
			checkRes: func(t *testing.T, res *InternalWorkflowSnapshot, err error) {
				assert.ErrorIs(t, err, assert.AnError)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			mockedSerializer := NewMockPayloadSerializer(ctrl)
			tc.prepareMocks(mockedSerializer)
			manager := NewExecutionManagerImpl(nil, testlogger.New(t), mockedSerializer).(*executionManagerImpl)
			res, err := manager.SerializeWorkflowSnapshot(tc.input, common.EncodingTypeThriftRW)
			tc.checkRes(t, res, err)
		})
	}
}

func TestDeserializeBufferedEvents(t *testing.T) {
	for _, tc := range []struct {
		name         string
		prepareMocks func(*MockPayloadSerializer)
		checkRes     func(*testing.T, []*types.HistoryEvent, error)
	}{
		{
			name: "success",
			prepareMocks: func(mockedSerializer *MockPayloadSerializer) {
				eventCounter := 0
				mockedSerializer.EXPECT().DeserializeBatchEvents(gomock.Any()).DoAndReturn(func(data *DataBlob) ([]*types.HistoryEvent, error) {
					res := []*types.HistoryEvent{{ID: int64(eventCounter)}, {ID: int64(eventCounter + 1)}}
					eventCounter += 2
					return res, nil
				}).Times(2)
			},
			checkRes: func(t *testing.T, events []*types.HistoryEvent, err error) {
				assert.NoError(t, err)
				assert.Equal(t, []*types.HistoryEvent{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}}, events)
			},
		},
		{
			name: "error",
			prepareMocks: func(mockedSerializer *MockPayloadSerializer) {
				mockedSerializer.EXPECT().DeserializeBatchEvents(gomock.Any()).Return(nil, assert.AnError).Times(1)
			},
			checkRes: func(t *testing.T, res []*types.HistoryEvent, err error) {
				assert.ErrorIs(t, err, assert.AnError)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockedSerializer := NewMockPayloadSerializer(ctrl)

			tc.prepareMocks(mockedSerializer)

			manager := NewExecutionManagerImpl(nil, testlogger.New(t), mockedSerializer).(*executionManagerImpl)

			events := []*DataBlob{
				sampleEventData(),
				sampleEventData(),
			}

			res, err := manager.DeserializeBufferedEvents(events)
			tc.checkRes(t, res, err)
		})
	}
}

func TestPutReplicationTaskToDLQ(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockedStore := NewMockExecutionStore(ctrl)
	manager := NewExecutionManagerImpl(mockedStore, testlogger.New(t), nil)

	now := time.Now().UTC().Round(time.Second)

	task := &PutReplicationTaskToDLQRequest{
		SourceClusterName: "test-cluster",
		TaskInfo: &ReplicationTaskInfo{
			DomainID:     testDomainID,
			WorkflowID:   testWorkflowID,
			CreationTime: now.UnixNano(),
		},
		DomainName: testDomain,
	}

	mockedStore.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, req *InternalPutReplicationTaskToDLQRequest) error {
		assert.Equal(t, &InternalPutReplicationTaskToDLQRequest{
			SourceClusterName: "test-cluster",
			TaskInfo: &InternalReplicationTaskInfo{
				DomainID:     testDomainID,
				WorkflowID:   testWorkflowID,
				CreationTime: now,
			},
		}, req)
		return nil
	})

	err := manager.PutReplicationTaskToDLQ(context.Background(), task)
	assert.NoError(t, err)
}

func TestGetReplicationTasksFromDLQ(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockedStore := NewMockExecutionStore(ctrl)
	manager := NewExecutionManagerImpl(mockedStore, testlogger.New(t), nil)

	request := &GetReplicationTasksFromDLQRequest{
		SourceClusterName: "test-cluster",
		GetReplicationTasksRequest: GetReplicationTasksRequest{
			ReadLevel:     1,
			MaxReadLevel:  2,
			BatchSize:     10,
			NextPageToken: nil,
		},
	}

	now := time.Now().UTC().Round(time.Second)

	mockedStore.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), request).Return(
		&InternalGetReplicationTasksFromDLQResponse{
			Tasks: []*InternalReplicationTaskInfo{
				{
					DomainID:     testDomainID,
					WorkflowID:   testWorkflowID,
					TaskID:       1,
					TaskType:     1,
					CreationTime: now,
				},
				{
					DomainID:     testDomainID,
					WorkflowID:   testWorkflowID,
					TaskID:       2,
					TaskType:     2,
					CreationTime: now.Add(time.Second),
				},
			},
			NextPageToken: []byte("test-token"),
		}, nil)

	res, err := manager.GetReplicationTasksFromDLQ(context.Background(), request)
	assert.NoError(t, err)
	assert.Equal(t, &GetReplicationTasksFromDLQResponse{
		Tasks: []*ReplicationTaskInfo{
			{
				DomainID:     testDomainID,
				WorkflowID:   testWorkflowID,
				TaskID:       1,
				TaskType:     1,
				CreationTime: now.UnixNano(),
			},
			{
				DomainID:     testDomainID,
				WorkflowID:   testWorkflowID,
				TaskID:       2,
				TaskType:     2,
				CreationTime: now.Add(time.Second).UnixNano(),
			},
		},
		NextPageToken: []byte("test-token"),
	}, res)
}

func TestDeserializeChildExecutionInfos(t *testing.T) {
	tests := []struct {
		name         string
		prepareMocks func(*MockPayloadSerializer)
		checkRes     func(*testing.T, map[int64]*ChildExecutionInfo, error)
	}{
		{
			name: "success",
			prepareMocks: func(mockedSerializer *MockPayloadSerializer) {
				// Child 1 init event
				mockedSerializer.EXPECT().DeserializeEvent(sampleEventDataWithVersion(1)).Return(&types.HistoryEvent{
					ID:      1,
					Version: 1,
				}, nil)
				// Child 1 start event
				mockedSerializer.EXPECT().DeserializeEvent(sampleEventDataWithVersion(2)).Return(&types.HistoryEvent{
					ID:      2,
					Version: 2,
				}, nil)
				// Child 2 init event
				mockedSerializer.EXPECT().DeserializeEvent(sampleEventDataWithVersion(3)).Return(&types.HistoryEvent{
					ID:      3,
					Version: 3,
				}, nil)

				// Child 2 start event is mimicking legacy behavior where runID and workflowID were not stored inside the info
				// but was extracted from the startEvent
				mockedSerializer.EXPECT().DeserializeEvent(sampleEventDataWithVersion(4)).Return(&types.HistoryEvent{
					ID:      4,
					Version: 4,
					ChildWorkflowExecutionStartedEventAttributes: &types.ChildWorkflowExecutionStartedEventAttributes{
						WorkflowExecution: &types.WorkflowExecution{
							WorkflowID: "legacy-workflow-id",
							RunID:      "legacy-run-id",
						},
					},
				}, nil)
			},
			checkRes: func(t *testing.T, events map[int64]*ChildExecutionInfo, err error) {
				assert.NoError(t, err)
				assert.Equal(t, map[int64]*ChildExecutionInfo{
					1: {
						Version:           1,
						DomainID:          testDomainID,
						WorkflowTypeName:  testWorkflowType,
						InitiatedID:       1,
						InitiatedEvent:    &types.HistoryEvent{ID: 1, Version: 1},
						StartedID:         2,
						StartedEvent:      &types.HistoryEvent{ID: 2, Version: 2},
						CreateRequestID:   "create-request-id",
						StartedWorkflowID: "workflow-id",
						StartedRunID:      "run-id",
					},
					2: {
						Version:          3,
						DomainID:         testDomainID,
						WorkflowTypeName: testWorkflowType,
						InitiatedID:      3,
						InitiatedEvent:   &types.HistoryEvent{ID: 3, Version: 3},
						StartedID:        4,
						StartedEvent: &types.HistoryEvent{ID: 4, Version: 4,
							ChildWorkflowExecutionStartedEventAttributes: &types.ChildWorkflowExecutionStartedEventAttributes{
								WorkflowExecution: &types.WorkflowExecution{
									WorkflowID: "legacy-workflow-id",
									RunID:      "legacy-run-id",
								},
							}},
						CreateRequestID:   "create-request-id",
						StartedWorkflowID: "legacy-workflow-id",
						StartedRunID:      "legacy-run-id",
					},
				}, events)
			},
		},
		{
			name: "deserialize event error",
			prepareMocks: func(mockedSerializer *MockPayloadSerializer) {
				mockedSerializer.EXPECT().DeserializeEvent(gomock.Any()).Return(nil, assert.AnError).Times(1)
			},
			checkRes: func(t *testing.T, events map[int64]*ChildExecutionInfo, err error) {
				assert.ErrorIs(t, err, assert.AnError)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			mockedSerializer := NewMockPayloadSerializer(ctrl)

			test.prepareMocks(mockedSerializer)

			manager := &executionManagerImpl{
				serializer: mockedSerializer,
			}

			result, err := manager.DeserializeChildExecutionInfos(map[int64]*InternalChildExecutionInfo{
				1: sampleInternalChildExecutionInfo(1, 2),
				2: sampleInternalChildExecutionInfo(3, 4),
			})
			test.checkRes(t, result, err)
		})
	}
}

func TestListConcreteExecutions(t *testing.T) {
	request := &ListConcreteExecutionsRequest{
		PageSize:  10,
		PageToken: []byte("next"),
	}

	internalResponse := &InternalListConcreteExecutionsResponse{
		Executions: []*InternalListConcreteExecutionsEntity{
			{
				ExecutionInfo:    sampleInternalWorkflowExecutionInfo(),
				VersionHistories: sampleEventData(),
			},
		},
		NextPageToken: []byte("next"),
	}

	testCases := []struct {
		name         string
		prepareMocks func(*MockExecutionStore, *MockPayloadSerializer)
		checkRes     func(*testing.T, *ListConcreteExecutionsResponse, error)
	}{
		{
			name: "success",
			prepareMocks: func(mockedStore *MockExecutionStore, mockedSerializer *MockPayloadSerializer) {
				mockedStore.EXPECT().ListConcreteExecutions(gomock.Any(), request).Return(internalResponse, nil)

				mockedSerializer.EXPECT().DeserializeEvent(internalResponse.Executions[0].ExecutionInfo.CompletionEvent).Return(completionEvent(), nil)
				mockedSerializer.EXPECT().DeserializeResetPoints(internalResponse.Executions[0].ExecutionInfo.AutoResetPoints).Return(&types.ResetPoints{
					Points: []*types.ResetPointInfo{
						{
							RunID: testRunID,
						},
					},
				}, nil)

				mockedSerializer.EXPECT().DeserializeVersionHistories(internalResponse.Executions[0].VersionHistories).Return(&types.VersionHistories{
					CurrentVersionHistoryIndex: 1,
					Histories: []*types.VersionHistory{
						{
							BranchToken: []byte("branch-token-1"),
							Items: []*types.VersionHistoryItem{
								{
									EventID: 1,
									Version: 1,
								},
							},
						},
						{
							BranchToken: []byte("branch-token-2"),
							Items: []*types.VersionHistoryItem{
								{
									EventID: 2,
									Version: 1,
								},
								{
									EventID: 3,
									Version: 2,
								},
							},
						},
					},
				}, nil)
			},
			checkRes: func(t *testing.T, response *ListConcreteExecutionsResponse, err error) {
				executionInfo := sampleWorkflowExecutionInfo()
				executionInfo.CompletionEvent = completionEvent()
				executionInfo.AutoResetPoints = &types.ResetPoints{
					Points: []*types.ResetPointInfo{
						{
							RunID: testRunID,
						},
					},
				}
				assert.Equal(t, &ListConcreteExecutionsResponse{
					Executions: []*ListConcreteExecutionsEntity{
						{
							ExecutionInfo: executionInfo,
							VersionHistories: &VersionHistories{
								CurrentVersionHistoryIndex: 1,
								Histories: []*VersionHistory{
									{
										BranchToken: []byte("branch-token-1"),
										Items: []*VersionHistoryItem{
											{
												EventID: 1,
												Version: 1,
											},
										},
									},
									{
										BranchToken: []byte("branch-token-2"),
										Items: []*VersionHistoryItem{
											{
												EventID: 2,
												Version: 1,
											},
											{
												EventID: 3,
												Version: 2,
											},
										},
									},
								},
							},
						},
					},
					PageToken: []byte("next"),
				}, response)
			},
		},
		{
			name: "persistence error",
			prepareMocks: func(mockedStore *MockExecutionStore, mockedSerializer *MockPayloadSerializer) {
				mockedStore.EXPECT().ListConcreteExecutions(gomock.Any(), request).Return(nil, assert.AnError)
			},
			checkRes: func(t *testing.T, response *ListConcreteExecutionsResponse, err error) {
				assert.ErrorIs(t, err, assert.AnError)
			},
		},
		{
			name: "deserialize execution info error",
			prepareMocks: func(mockedStore *MockExecutionStore, mockedSerializer *MockPayloadSerializer) {
				mockedStore.EXPECT().ListConcreteExecutions(gomock.Any(), request).Return(internalResponse, nil)
				mockedSerializer.EXPECT().DeserializeEvent(internalResponse.Executions[0].ExecutionInfo.CompletionEvent).Return(nil, assert.AnError)
			},
			checkRes: func(t *testing.T, response *ListConcreteExecutionsResponse, err error) {
				assert.ErrorIs(t, err, assert.AnError)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockedStore := NewMockExecutionStore(ctrl)
			mockedSerializer := NewMockPayloadSerializer(ctrl)

			tc.prepareMocks(mockedStore, mockedSerializer)

			manager := NewExecutionManagerImpl(mockedStore, testlogger.New(t), mockedSerializer)

			res, err := manager.ListConcreteExecutions(context.Background(), request)

			tc.checkRes(t, res, err)
		})
	}
}

func sampleInternalActivityInfo(name string) *InternalActivityInfo {
	return &InternalActivityInfo{
		Version:        1,
		ScheduleID:     1,
		ActivityID:     name,
		ScheduledEvent: NewDataBlob([]byte(fmt.Sprintf("%s-activity-scheduled-event", name)), common.EncodingTypeThriftRW),
		StartedEvent:   NewDataBlob([]byte(fmt.Sprintf("%s-activity-started-event", name)), common.EncodingTypeThriftRW),
	}
}

func sampleActivityInfo(name string, id int64) *ActivityInfo {
	return &ActivityInfo{
		Version:    1,
		ScheduleID: 1,
		ActivityID: name,
		ScheduledEvent: &types.HistoryEvent{
			ID: id,
		},
		StartedEvent: &types.HistoryEvent{
			ID: id,
		},
	}
}

var (
	startedTimestamp           = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	scheduledTimestamp         = time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC)
	originalScheduledTimestamp = time.Date(2020, 3, 1, 0, 0, 0, 0, time.UTC)

	wfTimeout       = 10 * time.Second
	decisionTimeout = 5 * time.Second
)

func sampleInternalWorkflowExecutionInfo() *InternalWorkflowExecutionInfo {
	return &InternalWorkflowExecutionInfo{
		DomainID:                           testDomain,
		WorkflowTimeout:                    wfTimeout,
		DecisionStartToCloseTimeout:        decisionTimeout,
		DecisionStartedTimestamp:           startedTimestamp,
		DecisionScheduledTimestamp:         scheduledTimestamp,
		DecisionOriginalScheduledTimestamp: originalScheduledTimestamp,
		WorkflowID:                         testWorkflowID,
		RunID:                              testRunID,
		WorkflowTypeName:                   testWorkflowType,
		NextEventID:                        10,
		CompletionEvent:                    sampleEventData(),
		AutoResetPoints:                    sampleResetPointsData(),
		HistorySize:                        1024,
	}
}

func sampleWorkflowExecutionInfo() *WorkflowExecutionInfo {
	return &WorkflowExecutionInfo{
		DomainID:                           testDomain,
		WorkflowTimeout:                    int32(wfTimeout.Seconds()),
		DecisionStartToCloseTimeout:        int32(decisionTimeout.Seconds()),
		DecisionScheduledTimestamp:         scheduledTimestamp.UnixNano(),
		DecisionStartedTimestamp:           startedTimestamp.UnixNano(),
		DecisionOriginalScheduledTimestamp: originalScheduledTimestamp.UnixNano(),
		WorkflowID:                         testWorkflowID,
		RunID:                              testRunID,
		WorkflowTypeName:                   testWorkflowType,
		NextEventID:                        10,
		CompletionEvent:                    completionEvent(),
	}
}

func sampleInternalWorkflowMutation() *InternalWorkflowMutation {
	return &InternalWorkflowMutation{
		ExecutionInfo: &InternalWorkflowExecutionInfo{
			DomainID:                           testDomain,
			WorkflowTimeout:                    wfTimeout,
			DecisionStartToCloseTimeout:        decisionTimeout,
			DecisionStartedTimestamp:           startedTimestamp,
			DecisionScheduledTimestamp:         scheduledTimestamp,
			DecisionOriginalScheduledTimestamp: originalScheduledTimestamp,
			WorkflowID:                         testWorkflowID,
			RunID:                              testRunID,
			WorkflowTypeName:                   testWorkflowType,
			NextEventID:                        10,
		},
	}
}

func sampleWorkflowSnapshot() *WorkflowSnapshot {
	return &WorkflowSnapshot{
		ExecutionInfo:  sampleWorkflowExecutionInfo(),
		ExecutionStats: sampleWorkflowExecutionStats(),
		VersionHistories: &VersionHistories{
			CurrentVersionHistoryIndex: 0,
			Histories: []*VersionHistory{
				{
					BranchToken: []byte("test-branch-token"),
					Items: []*VersionHistoryItem{
						{
							EventID: 1,
							Version: 1,
						},
					},
				},
			},
		},
		Checksum: generateChecksum(),
		ActivityInfos: []*ActivityInfo{
			{
				Version:        1,
				ScheduleID:     1,
				ActivityID:     "activity1",
				ScheduledEvent: activityScheduledEvent(),
				StartedID:      2,
				StartedEvent:   activityStartedEvent(),
				StartedTime:    startedTimestamp,
			},
		},
		ChildExecutionInfos: []*ChildExecutionInfo{
			{
				Version:          1,
				InitiatedID:      1,
				InitiatedEvent:   childWorkflowScheduledEvent(),
				StartedID:        2,
				StartedEvent:     childWorkflowStartedEvent(),
				CreateRequestID:  "create-request-id",
				DomainID:         testDomainID,
				WorkflowTypeName: testWorkflowType,
			},
		},
		TimerInfos: []*TimerInfo{
			{
				TimerID:    "test-timer",
				StartedID:  1,
				ExpiryTime: originalScheduledTimestamp,
				TaskStatus: 1,
			},
			{
				TimerID:    "test-timer-2",
				StartedID:  2,
				ExpiryTime: originalScheduledTimestamp,
				TaskStatus: 2,
			},
		},
	}
}

func sampleInternalWorkflowSnapshot() *InternalWorkflowSnapshot {
	return &InternalWorkflowSnapshot{
		ExecutionInfo:    sampleInternalWorkflowExecutionInfo(),
		VersionHistories: sampleEventData(),
		StartVersion:     1,
		LastWriteVersion: 1,
		ActivityInfos: []*InternalActivityInfo{
			{
				Version:        1,
				ScheduleID:     1,
				ActivityID:     "activity1",
				ScheduledEvent: sampleEventData(),
				StartedEvent:   sampleEventData(),
				StartedID:      2,
				StartedTime:    startedTimestamp,
			},
		},
		TimerInfos: []*TimerInfo{
			{
				TimerID:    "test-timer",
				StartedID:  1,
				ExpiryTime: originalScheduledTimestamp,
				TaskStatus: 1,
			},
			{
				TimerID:    "test-timer-2",
				StartedID:  2,
				ExpiryTime: originalScheduledTimestamp,
				TaskStatus: 2,
			},
		},
		ChildExecutionInfos: []*InternalChildExecutionInfo{
			{
				Version:          1,
				InitiatedID:      1,
				InitiatedEvent:   sampleEventData(),
				StartedID:        2,
				StartedEvent:     sampleEventData(),
				CreateRequestID:  "create-request-id",
				DomainID:         testDomainID,
				WorkflowTypeName: testWorkflowType,
			},
		},
		Checksum:     generateChecksum(),
		ChecksumData: sampleCheckSumData(),
	}
}

func activityScheduledEvent() *types.HistoryEvent {
	return &types.HistoryEvent{
		ID:        1,
		Timestamp: common.Ptr(scheduledTimestamp.UnixNano()),
		TaskID:    1,
	}
}

func activityStartedEvent() *types.HistoryEvent {
	return &types.HistoryEvent{
		ID:        2,
		Timestamp: common.Ptr(startedTimestamp.UnixNano()),
		TaskID:    1,
	}
}

func childWorkflowScheduledEvent() *types.HistoryEvent {
	return &types.HistoryEvent{
		ID:        1,
		Timestamp: common.Ptr(scheduledTimestamp.UnixNano()),
		TaskID:    1,
	}
}

func completionEvent() *types.HistoryEvent {
	return &types.HistoryEvent{
		ID:        99,
		Timestamp: common.Ptr(startedTimestamp.UnixNano()),
		TaskID:    1,
	}
}

func childWorkflowStartedEvent() *types.HistoryEvent {
	return &types.HistoryEvent{
		ID:        2,
		Timestamp: common.Ptr(startedTimestamp.UnixNano()),
		TaskID:    1,
	}
}

func sampleWorkflowExecutionStats() *ExecutionStats {
	return &ExecutionStats{
		HistorySize: 1024,
	}
}

func sampleTestCheckSumData() *DataBlob {
	return &DataBlob{
		Encoding: common.EncodingTypeThriftRW,
		Data:     []byte("test-checksum"),
	}
}

func sampleEventData() *DataBlob {
	return NewDataBlob([]byte("test-event"), common.EncodingTypeThriftRW)
}

func sampleResetPointsData() *DataBlob {
	return NewDataBlob([]byte("test-reset-points"), common.EncodingTypeThriftRW)
}

func sampleCheckSumData() *DataBlob {
	return NewDataBlob([]byte("test-checksum"), common.EncodingTypeThriftRW)
}

func sampleInternalChildExecutionInfo(initEventID, startedEventID int64) *InternalChildExecutionInfo {
	return &InternalChildExecutionInfo{
		Version:           initEventID,
		InitiatedID:       initEventID,
		InitiatedEvent:    sampleEventDataWithVersion(initEventID),
		StartedID:         startedEventID,
		StartedEvent:      sampleEventDataWithVersion(startedEventID),
		CreateRequestID:   "create-request-id",
		DomainID:          testDomainID,
		WorkflowTypeName:  testWorkflowType,
		StartedWorkflowID: "workflow-id",
		StartedRunID:      "run-id",
	}
}

func sampleEventDataWithVersion(i int64) *DataBlob {
	return NewDataBlob([]byte(fmt.Sprintf("test-event-%d", i)), common.EncodingTypeThriftRW)
}
