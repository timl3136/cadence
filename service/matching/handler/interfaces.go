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

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination interfaces_mock.go -package handler github.com/uber/cadence/service/matching/handler Handler
//go:generate gowrap gen -g -p . -i Handler -t ../../templates/grpc.tmpl -o ../wrappers/grpc/grpc_handler_generated.go -v handler=GRPC -v package=matchingv1 -v path=github.com/uber/cadence/.gen/proto/matching/v1 -v prefix=Matching
//go:generate gowrap gen -g -p ../../../.gen/go/matching/matchingserviceserver -i Interface -t ../../templates/thrift.tmpl -o ../wrappers/thrift/thrift_handler_generated.go -v handler=Thrift -v prefix=Matching

package handler

import (
	"context"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/types"
)

type (
	// Engine exposes interfaces for clients to poll for activity and decision tasks.
	Engine interface {
		Stop()
		AddDecisionTask(hCtx *handlerContext, request *types.AddDecisionTaskRequest) (syncMatch bool, err error)
		AddActivityTask(hCtx *handlerContext, request *types.AddActivityTaskRequest) (syncMatch bool, err error)
		PollForDecisionTask(hCtx *handlerContext, request *types.MatchingPollForDecisionTaskRequest) (*types.MatchingPollForDecisionTaskResponse, error)
		PollForActivityTask(hCtx *handlerContext, request *types.MatchingPollForActivityTaskRequest) (*types.PollForActivityTaskResponse, error)
		QueryWorkflow(hCtx *handlerContext, request *types.MatchingQueryWorkflowRequest) (*types.QueryWorkflowResponse, error)
		RespondQueryTaskCompleted(hCtx *handlerContext, request *types.MatchingRespondQueryTaskCompletedRequest) error
		CancelOutstandingPoll(hCtx *handlerContext, request *types.CancelOutstandingPollRequest) error
		DescribeTaskList(hCtx *handlerContext, request *types.MatchingDescribeTaskListRequest) (*types.DescribeTaskListResponse, error)
		ListTaskListPartitions(hCtx *handlerContext, request *types.MatchingListTaskListPartitionsRequest) (*types.ListTaskListPartitionsResponse, error)
		GetTaskListsByDomain(hCtx *handlerContext, request *types.GetTaskListsByDomainRequest) (*types.GetTaskListsByDomainResponse, error)
	}

	// Handler interface for matching service
	Handler interface {
		common.Daemon

		Health(context.Context) (*types.HealthStatus, error)
		AddActivityTask(context.Context, *types.AddActivityTaskRequest) error
		AddDecisionTask(context.Context, *types.AddDecisionTaskRequest) error
		CancelOutstandingPoll(context.Context, *types.CancelOutstandingPollRequest) error
		DescribeTaskList(context.Context, *types.MatchingDescribeTaskListRequest) (*types.DescribeTaskListResponse, error)
		ListTaskListPartitions(context.Context, *types.MatchingListTaskListPartitionsRequest) (*types.ListTaskListPartitionsResponse, error)
		GetTaskListsByDomain(context.Context, *types.GetTaskListsByDomainRequest) (*types.GetTaskListsByDomainResponse, error)
		PollForActivityTask(context.Context, *types.MatchingPollForActivityTaskRequest) (*types.PollForActivityTaskResponse, error)
		PollForDecisionTask(context.Context, *types.MatchingPollForDecisionTaskRequest) (*types.MatchingPollForDecisionTaskResponse, error)
		QueryWorkflow(context.Context, *types.MatchingQueryWorkflowRequest) (*types.QueryWorkflowResponse, error)
		RespondQueryTaskCompleted(context.Context, *types.MatchingRespondQueryTaskCompletedRequest) error
	}
)
