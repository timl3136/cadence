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

package handler

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/client"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/matching/config"
	"github.com/uber/cadence/service/matching/tasklist"
)

func TestGetTaskListsByDomain(t *testing.T) {
	testCases := []struct {
		name      string
		mockSetup func(*cache.MockDomainCache, map[tasklist.Identifier]*tasklist.MockManager, map[tasklist.Identifier]*tasklist.MockManager)
		wantErr   bool
		want      *types.GetTaskListsByDomainResponse
	}{
		{
			name: "domain cache error",
			mockSetup: func(mockDomainCache *cache.MockDomainCache, mockTaskListManagers map[tasklist.Identifier]*tasklist.MockManager, mockStickyManagers map[tasklist.Identifier]*tasklist.MockManager) {
				mockDomainCache.EXPECT().GetDomainID("test-domain").Return("", errors.New("cache failure"))
			},
			wantErr: true,
		},
		{
			name: "success",
			mockSetup: func(mockDomainCache *cache.MockDomainCache, mockTaskListManagers map[tasklist.Identifier]*tasklist.MockManager, mockStickyManagers map[tasklist.Identifier]*tasklist.MockManager) {
				mockDomainCache.EXPECT().GetDomainID("test-domain").Return("test-domain-id", nil)
				for id, mockManager := range mockTaskListManagers {
					if id.GetDomainID() == "test-domain-id" {
						mockManager.EXPECT().GetTaskListKind().Return(types.TaskListKindNormal)
						mockManager.EXPECT().DescribeTaskList(false).Return(&types.DescribeTaskListResponse{
							Pollers: []*types.PollerInfo{
								{
									Identity: fmt.Sprintf("test-poller-%s", id.GetRoot()),
								},
							},
						})
					}
				}
				for id, mockManager := range mockStickyManagers {
					if id.GetDomainID() == "test-domain-id" {
						mockManager.EXPECT().GetTaskListKind().Return(types.TaskListKindSticky)
					}
				}
			},
			wantErr: false,
			want: &types.GetTaskListsByDomainResponse{
				DecisionTaskListMap: map[string]*types.DescribeTaskListResponse{
					"decision0": {
						Pollers: []*types.PollerInfo{
							{
								Identity: "test-poller-decision0",
							},
						},
					},
				},
				ActivityTaskListMap: map[string]*types.DescribeTaskListResponse{
					"activity0": {
						Pollers: []*types.PollerInfo{
							{
								Identity: "test-poller-activity0",
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			mockDomainCache := cache.NewMockDomainCache(mockCtrl)
			decisionTasklistID, err := tasklist.NewIdentifier("test-domain-id", "decision0", 0)
			require.NoError(t, err)
			activityTasklistID, err := tasklist.NewIdentifier("test-domain-id", "activity0", 1)
			require.NoError(t, err)
			otherDomainTasklistID, err := tasklist.NewIdentifier("other-domain-id", "other0", 0)
			require.NoError(t, err)
			mockDecisionTaskListManager := tasklist.NewMockManager(mockCtrl)
			mockActivityTaskListManager := tasklist.NewMockManager(mockCtrl)
			mockOtherDomainTaskListManager := tasklist.NewMockManager(mockCtrl)
			mockTaskListManagers := map[tasklist.Identifier]*tasklist.MockManager{
				*decisionTasklistID:    mockDecisionTaskListManager,
				*activityTasklistID:    mockActivityTaskListManager,
				*otherDomainTasklistID: mockOtherDomainTaskListManager,
			}
			stickyTasklistID, err := tasklist.NewIdentifier("test-domain-id", "sticky0", 0)
			require.NoError(t, err)
			mockStickyManager := tasklist.NewMockManager(mockCtrl)
			mockStickyManagers := map[tasklist.Identifier]*tasklist.MockManager{
				*stickyTasklistID: mockStickyManager,
			}
			tc.mockSetup(mockDomainCache, mockTaskListManagers, mockStickyManagers)

			engine := &matchingEngineImpl{
				domainCache: mockDomainCache,
				taskLists: map[tasklist.Identifier]tasklist.Manager{
					*decisionTasklistID:    mockDecisionTaskListManager,
					*activityTasklistID:    mockActivityTaskListManager,
					*otherDomainTasklistID: mockOtherDomainTaskListManager,
					*stickyTasklistID:      mockStickyManager,
				},
			}
			resp, err := engine.GetTaskListsByDomain(nil, &types.GetTaskListsByDomainRequest{Domain: "test-domain"})

			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.want, resp)
			}
		})
	}
}

func TestListTaskListPartitions(t *testing.T) {
	testCases := []struct {
		name      string
		req       *types.MatchingListTaskListPartitionsRequest
		mockSetup func(*cache.MockDomainCache, *membership.MockResolver)
		wantErr   bool
		want      *types.ListTaskListPartitionsResponse
	}{
		{
			name: "domain cache error",
			req: &types.MatchingListTaskListPartitionsRequest{
				Domain: "test-domain",
				TaskList: &types.TaskList{
					Name: "test-tasklist",
				},
			},
			mockSetup: func(mockDomainCache *cache.MockDomainCache, mockResolver *membership.MockResolver) {
				mockDomainCache.EXPECT().GetDomainID("test-domain").Return("", errors.New("cache failure"))
			},
			wantErr: true,
		},
		{
			name: "invalid tasklist name",
			req: &types.MatchingListTaskListPartitionsRequest{
				Domain: "test-domain",
				TaskList: &types.TaskList{
					Name: "/__cadence_sys/invalid-tasklist-name",
				},
			},
			mockSetup: func(mockDomainCache *cache.MockDomainCache, mockResolver *membership.MockResolver) {
				mockDomainCache.EXPECT().GetDomainID("test-domain").Return("test-domain-id", nil)
			},
			wantErr: true,
		},
		{
			name: "success",
			req: &types.MatchingListTaskListPartitionsRequest{
				Domain: "test-domain",
				TaskList: &types.TaskList{
					Name: "test-tasklist",
				},
			},
			mockSetup: func(mockDomainCache *cache.MockDomainCache, mockResolver *membership.MockResolver) {
				// activity tasklist
				mockDomainCache.EXPECT().GetDomainID("test-domain").Return("test-domain-id", nil)
				mockResolver.EXPECT().Lookup(gomock.Any(), "test-tasklist").Return(membership.NewHostInfo("addr2"), nil)
				mockResolver.EXPECT().Lookup(gomock.Any(), "/__cadence_sys/test-tasklist/1").Return(membership.HostInfo{}, errors.New("some error"))
				mockResolver.EXPECT().Lookup(gomock.Any(), "/__cadence_sys/test-tasklist/2").Return(membership.NewHostInfo("addr3"), nil)
				// decision tasklist
				mockDomainCache.EXPECT().GetDomainID("test-domain").Return("test-domain-id", nil)
				mockResolver.EXPECT().Lookup(gomock.Any(), "test-tasklist").Return(membership.NewHostInfo("addr0"), nil)
				mockResolver.EXPECT().Lookup(gomock.Any(), "/__cadence_sys/test-tasklist/1").Return(membership.HostInfo{}, errors.New("some error"))
				mockResolver.EXPECT().Lookup(gomock.Any(), "/__cadence_sys/test-tasklist/2").Return(membership.NewHostInfo("addr1"), nil)
			},
			wantErr: false,
			want: &types.ListTaskListPartitionsResponse{
				DecisionTaskListPartitions: []*types.TaskListPartitionMetadata{
					{
						Key:           "test-tasklist",
						OwnerHostName: "addr0",
					},
					{
						Key:           "/__cadence_sys/test-tasklist/1",
						OwnerHostName: "",
					},
					{
						Key:           "/__cadence_sys/test-tasklist/2",
						OwnerHostName: "addr1",
					},
				},
				ActivityTaskListPartitions: []*types.TaskListPartitionMetadata{
					{
						Key:           "test-tasklist",
						OwnerHostName: "addr2",
					},
					{
						Key:           "/__cadence_sys/test-tasklist/1",
						OwnerHostName: "",
					},
					{
						Key:           "/__cadence_sys/test-tasklist/2",
						OwnerHostName: "addr3",
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			mockDomainCache := cache.NewMockDomainCache(mockCtrl)
			mockResolver := membership.NewMockResolver(mockCtrl)
			tc.mockSetup(mockDomainCache, mockResolver)

			engine := &matchingEngineImpl{
				domainCache:        mockDomainCache,
				membershipResolver: mockResolver,
				config: &config.Config{
					NumTasklistWritePartitions: dynamicconfig.GetIntPropertyFilteredByTaskListInfo(3),
				},
			}
			resp, err := engine.ListTaskListPartitions(nil, tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.want, resp)
			}
		})
	}
}

func TestCancelOutstandingPoll(t *testing.T) {
	testCases := []struct {
		name      string
		req       *types.CancelOutstandingPollRequest
		mockSetup func(*tasklist.MockManager)
		wantErr   bool
	}{
		{
			name: "invalid tasklist name",
			req: &types.CancelOutstandingPollRequest{
				DomainUUID: "test-domain-id",
				TaskList: &types.TaskList{
					Name: "/__cadence_sys/invalid-tasklist-name",
				},
				PollerID: "test-poller-id",
			},
			mockSetup: func(mockManager *tasklist.MockManager) {
			},
			wantErr: true,
		},
		{
			name: "success",
			req: &types.CancelOutstandingPollRequest{
				DomainUUID: "test-domain-id",
				TaskList: &types.TaskList{
					Name: "test-tasklist",
				},
				PollerID: "test-poller-id",
			},
			mockSetup: func(mockManager *tasklist.MockManager) {
				mockManager.EXPECT().CancelPoller("test-poller-id")
			},
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			mockManager := tasklist.NewMockManager(mockCtrl)
			tc.mockSetup(mockManager)
			tasklistID, err := tasklist.NewIdentifier("test-domain-id", "test-tasklist", 0)
			require.NoError(t, err)
			engine := &matchingEngineImpl{
				taskLists: map[tasklist.Identifier]tasklist.Manager{
					*tasklistID: mockManager,
				},
			}
			err = engine.CancelOutstandingPoll(nil, tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRespondQueryTaskCompleted(t *testing.T) {
	testCases := []struct {
		name         string
		req          *types.MatchingRespondQueryTaskCompletedRequest
		queryTaskMap map[string]chan *queryResult
		wantErr      bool
	}{
		{
			name: "success",
			req: &types.MatchingRespondQueryTaskCompletedRequest{
				DomainUUID: "test-domain-id",
				TaskList: &types.TaskList{
					Name: "test-tasklist",
				},
				TaskID: "id-0",
			},
			queryTaskMap: map[string]chan *queryResult{
				"id-0": make(chan *queryResult, 1),
			},
			wantErr: false,
		},
		{
			name: "query task not found",
			req: &types.MatchingRespondQueryTaskCompletedRequest{
				DomainUUID: "test-domain-id",
				TaskList: &types.TaskList{
					Name: "test-tasklist",
				},
				TaskID: "id-0",
			},
			queryTaskMap: map[string]chan *queryResult{},
			wantErr:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			engine := &matchingEngineImpl{
				lockableQueryTaskMap: lockableQueryTaskMap{
					queryTaskMap: tc.queryTaskMap,
				},
			}
			err := engine.RespondQueryTaskCompleted(&handlerContext{scope: metrics.NewNoopMetricsClient().Scope(0)}, tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestQueryWorkflow(t *testing.T) {
	testCases := []struct {
		name                 string
		req                  *types.MatchingQueryWorkflowRequest
		hCtx                 *handlerContext
		mockSetup            func(*tasklist.MockManager)
		waitForQueryResultFn func(hCtx *handlerContext, isStrongConsistencyQuery bool, queryResultCh <-chan *queryResult) (*types.QueryWorkflowResponse, error)
		wantErr              bool
		want                 *types.QueryWorkflowResponse
	}{
		{
			name: "invalid tasklist name",
			req: &types.MatchingQueryWorkflowRequest{
				DomainUUID: "test-domain-id",
				TaskList: &types.TaskList{
					Name: "/__cadence_sys/invalid-tasklist-name",
				},
			},
			mockSetup: func(mockManager *tasklist.MockManager) {},
			wantErr:   true,
		},
		{
			name: "sticky worker unavailable",
			req: &types.MatchingQueryWorkflowRequest{
				DomainUUID: "test-domain-id",
				TaskList: &types.TaskList{
					Name: "test-tasklist",
					Kind: types.TaskListKindSticky.Ptr(),
				},
			},
			mockSetup: func(mockManager *tasklist.MockManager) {
				mockManager.EXPECT().HasPollerAfter(gomock.Any()).Return(false)
			},
			wantErr: true,
		},
		{
			name: "failed to dispatch query task",
			req: &types.MatchingQueryWorkflowRequest{
				DomainUUID: "test-domain-id",
				TaskList: &types.TaskList{
					Name: "test-tasklist",
				},
			},
			hCtx: &handlerContext{
				Context: context.Background(),
			},
			mockSetup: func(mockManager *tasklist.MockManager) {
				mockManager.EXPECT().DispatchQueryTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("some error"))
			},
			wantErr: true,
		},
		{
			name: "success",
			req: &types.MatchingQueryWorkflowRequest{
				DomainUUID: "test-domain-id",
				TaskList: &types.TaskList{
					Name: "test-tasklist",
				},
			},
			hCtx: &handlerContext{
				Context: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					return ctx
				}(),
			},
			mockSetup: func(mockManager *tasklist.MockManager) {
				mockManager.EXPECT().DispatchQueryTask(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
			},
			waitForQueryResultFn: func(hCtx *handlerContext, isStrongConsistencyQuery bool, queryResultCh <-chan *queryResult) (*types.QueryWorkflowResponse, error) {
				return &types.QueryWorkflowResponse{
					QueryResult: []byte("some result"),
				}, nil
			},
			wantErr: false,
			want: &types.QueryWorkflowResponse{
				QueryResult: []byte("some result"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			mockManager := tasklist.NewMockManager(mockCtrl)
			tc.mockSetup(mockManager)
			tasklistID, err := tasklist.NewIdentifier("test-domain-id", "test-tasklist", 0)
			require.NoError(t, err)
			engine := &matchingEngineImpl{
				taskLists: map[tasklist.Identifier]tasklist.Manager{
					*tasklistID: mockManager,
				},
				timeSource:           clock.NewRealTimeSource(),
				lockableQueryTaskMap: lockableQueryTaskMap{queryTaskMap: make(map[string]chan *queryResult)},
				waitForQueryResultFn: tc.waitForQueryResultFn,
			}
			resp, err := engine.QueryWorkflow(tc.hCtx, tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.want, resp)
			}
		})
	}
}

func TestWaitForQueryResult(t *testing.T) {
	testCases := []struct {
		name      string
		result    *queryResult
		mockSetup func(*client.VersionCheckerMock)
		wantErr   bool
		assertErr func(*testing.T, error)
		want      *types.QueryWorkflowResponse
	}{
		{
			name: "internal error",
			result: &queryResult{
				internalError: errors.New("some error"),
			},
			mockSetup: func(mockVersionChecker *client.VersionCheckerMock) {},
			assertErr: func(t *testing.T, err error) {
				assert.Equal(t, "some error", err.Error())
			},
			wantErr: true,
		},
		{
			name: "strong consistency query not supported",
			result: &queryResult{
				workerResponse: &types.MatchingRespondQueryTaskCompletedRequest{
					CompletedRequest: &types.RespondQueryTaskCompletedRequest{
						WorkerVersionInfo: &types.WorkerVersionInfo{
							Impl:           "uber-go",
							FeatureVersion: "1.0.0",
						},
					},
				},
			},
			mockSetup: func(mockVersionChecker *client.VersionCheckerMock) {
				mockVersionChecker.EXPECT().SupportsConsistentQuery("uber-go", "1.0.0").Return(errors.New("version error"))
			},
			assertErr: func(t *testing.T, err error) {
				assert.Equal(t, "version error", err.Error())
			},
			wantErr: true,
		},
		{
			name: "success - query task completed",
			result: &queryResult{
				workerResponse: &types.MatchingRespondQueryTaskCompletedRequest{
					CompletedRequest: &types.RespondQueryTaskCompletedRequest{
						WorkerVersionInfo: &types.WorkerVersionInfo{
							Impl:           "uber-go",
							FeatureVersion: "1.0.0",
						},
						CompletedType: types.QueryTaskCompletedTypeCompleted.Ptr(),
						QueryResult:   []byte("some result"),
					},
				},
			},
			mockSetup: func(mockVersionChecker *client.VersionCheckerMock) {
				mockVersionChecker.EXPECT().SupportsConsistentQuery("uber-go", "1.0.0").Return(nil)
			},
			wantErr: false,
			want: &types.QueryWorkflowResponse{
				QueryResult: []byte("some result"),
			},
		},
		{
			name: "query task failed",
			result: &queryResult{
				workerResponse: &types.MatchingRespondQueryTaskCompletedRequest{
					CompletedRequest: &types.RespondQueryTaskCompletedRequest{
						WorkerVersionInfo: &types.WorkerVersionInfo{
							Impl:           "uber-go",
							FeatureVersion: "1.0.0",
						},
						CompletedType: types.QueryTaskCompletedTypeFailed.Ptr(),
						ErrorMessage:  "query failed",
					},
				},
			},
			mockSetup: func(mockVersionChecker *client.VersionCheckerMock) {
				mockVersionChecker.EXPECT().SupportsConsistentQuery("uber-go", "1.0.0").Return(nil)
			},
			assertErr: func(t *testing.T, err error) {
				var e *types.QueryFailedError
				assert.ErrorAs(t, err, &e)
				assert.Equal(t, "query failed", e.Message)
			},
			wantErr: true,
		},
		{
			name: "unknown query result",
			result: &queryResult{
				workerResponse: &types.MatchingRespondQueryTaskCompletedRequest{
					CompletedRequest: &types.RespondQueryTaskCompletedRequest{
						WorkerVersionInfo: &types.WorkerVersionInfo{
							Impl:           "uber-go",
							FeatureVersion: "1.0.0",
						},
						CompletedType: types.QueryTaskCompletedType(100).Ptr(),
					},
				},
			},
			mockSetup: func(mockVersionChecker *client.VersionCheckerMock) {
				mockVersionChecker.EXPECT().SupportsConsistentQuery("uber-go", "1.0.0").Return(nil)
			},
			assertErr: func(t *testing.T, err error) {
				var e *types.InternalServiceError
				assert.ErrorAs(t, err, &e)
				assert.Equal(t, "unknown query completed type", e.Message)
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			mockVersionChecker := client.NewMockVersionChecker(mockCtrl)
			tc.mockSetup(mockVersionChecker)
			engine := &matchingEngineImpl{
				versionChecker: mockVersionChecker,
			}
			hCtx := &handlerContext{
				Context: context.Background(),
			}
			ch := make(chan *queryResult, 1)
			ch <- tc.result
			resp, err := engine.waitForQueryResult(hCtx, true, ch)
			if tc.wantErr {
				require.Error(t, err)
				if tc.assertErr != nil {
					tc.assertErr(t, err)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.want, resp)
			}
		})
	}
}
