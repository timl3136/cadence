// Copyright (c) 2017-2020 Uber Technologies, Inc.
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

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/yarpc"
	"golang.org/x/sync/errgroup"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/.gen/go/sqlblobs"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/client"
	"github.com/uber/cadence/common/codec"
	"github.com/uber/cadence/common/domain"
	"github.com/uber/cadence/common/elasticsearch/validator"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/partition"
	"github.com/uber/cadence/common/persistence"
	persistenceutils "github.com/uber/cadence/common/persistence/persistence-utils"
	"github.com/uber/cadence/common/resource"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/common/types/mapper/thrift"
	"github.com/uber/cadence/service/frontend/config"
	"github.com/uber/cadence/service/frontend/validate"
)

const (
	// HealthStatusOK is used when this node is healthy and rpc requests are allowed
	HealthStatusOK HealthStatus = iota + 1
	// HealthStatusWarmingUp is used when the rpc handler is warming up
	HealthStatusWarmingUp
	// HealthStatusShuttingDown is used when the rpc handler is shutting down
	HealthStatusShuttingDown
)

var _ Handler = (*WorkflowHandler)(nil)

type (
	// WorkflowHandler - Thrift handler interface for workflow service
	WorkflowHandler struct {
		resource.Resource

		shuttingDown              int32
		healthStatus              int32
		tokenSerializer           common.TaskTokenSerializer
		config                    *config.Config
		versionChecker            client.VersionChecker
		domainHandler             domain.Handler
		visibilityQueryValidator  *validator.VisibilityQueryValidator
		searchAttributesValidator *validator.SearchAttributesValidator
		throttleRetry             *backoff.ThrottleRetry
		producerManager           ProducerManager
		thriftrwEncoder           codec.BinaryEncoder
	}

	getHistoryContinuationToken struct {
		RunID             string
		FirstEventID      int64
		NextEventID       int64
		IsWorkflowRunning bool
		PersistenceToken  []byte
		TransientDecision *types.TransientDecisionInfo
		BranchToken       []byte
	}

	domainGetter interface {
		GetDomain() string
	}

	// HealthStatus is an enum that refers to the rpc handler health status
	HealthStatus int32
)

var (
	frontendServiceRetryPolicy = common.CreateFrontendServiceRetryPolicy()
)

// NewWorkflowHandler creates a thrift handler for the cadence service
func NewWorkflowHandler(
	resource resource.Resource,
	config *config.Config,
	versionChecker client.VersionChecker,
	domainHandler domain.Handler,
) *WorkflowHandler {
	return &WorkflowHandler{
		Resource:        resource,
		config:          config,
		healthStatus:    int32(HealthStatusWarmingUp),
		tokenSerializer: common.NewJSONTaskTokenSerializer(),
		versionChecker:  versionChecker,
		domainHandler:   domainHandler,
		visibilityQueryValidator: validator.NewQueryValidator(
			config.ValidSearchAttributes,
			config.EnableQueryAttributeValidation,
		),
		searchAttributesValidator: validator.NewSearchAttributesValidator(
			resource.GetLogger(),
			config.EnableQueryAttributeValidation,
			config.ValidSearchAttributes,
			config.SearchAttributesNumberOfKeysLimit,
			config.SearchAttributesSizeOfValueLimit,
			config.SearchAttributesTotalSizeLimit,
		),
		throttleRetry: backoff.NewThrottleRetry(
			backoff.WithRetryPolicy(frontendServiceRetryPolicy),
			backoff.WithRetryableError(common.IsServiceTransientError),
		),
		producerManager: NewProducerManager(
			resource.GetDomainCache(),
			resource.GetAsyncWorkflowQueueProvider(),
			resource.GetLogger(),
			resource.GetMetricsClient(),
		),
		thriftrwEncoder: codec.NewThriftRWEncoder(),
	}
}

// Start starts the handler
func (wh *WorkflowHandler) Start() {
	// TODO: Get warmup duration from config. Even better, run proactive checks such as probing downstream connections.
	const warmUpDuration = 30 * time.Second

	warmupTimer := time.NewTimer(warmUpDuration)
	go func() {
		<-warmupTimer.C
		wh.GetLogger().Warn("Service warmup duration has elapsed.")
		if atomic.CompareAndSwapInt32(&wh.healthStatus, int32(HealthStatusWarmingUp), int32(HealthStatusOK)) {
			wh.GetLogger().Warn("Warmup time has elapsed. Service is healthy.")
		} else {
			status := HealthStatus(atomic.LoadInt32(&wh.healthStatus))
			wh.GetLogger().Warn(fmt.Sprintf("Warmup time has elapsed. Service status is: %v", status.String()))
		}
	}()
}

// Stop stops the handler
func (wh *WorkflowHandler) Stop() {
	atomic.StoreInt32(&wh.shuttingDown, 1)
}

// UpdateHealthStatus sets the health status for this rpc handler.
// This health status will be used within the rpc health check handler
func (wh *WorkflowHandler) UpdateHealthStatus(status HealthStatus) {
	atomic.StoreInt32(&wh.healthStatus, int32(status))
}

func (wh *WorkflowHandler) isShuttingDown() bool {
	return atomic.LoadInt32(&wh.shuttingDown) != 0
}

// Health is for health check
func (wh *WorkflowHandler) Health(ctx context.Context) (*types.HealthStatus, error) {
	status := HealthStatus(atomic.LoadInt32(&wh.healthStatus))
	msg := status.String()

	if status != HealthStatusOK {
		wh.GetLogger().Warn(fmt.Sprintf("Service status is: %v", msg))
	}

	return &types.HealthStatus{
		Ok:  status == HealthStatusOK,
		Msg: msg,
	}, nil
}

// RegisterDomain creates a new domain which can be used as a container for all resources.  Domain is a top level
// entity within Cadence, used as a container for all resources like workflow executions, tasklists, etc.  Domain
// acts as a sandbox and provides isolation for all resources within the domain.  All resources belongs to exactly one
// domain.
func (wh *WorkflowHandler) RegisterDomain(ctx context.Context, registerRequest *types.RegisterDomainRequest) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return err
	}

	if registerRequest == nil {
		return validate.ErrRequestNotSet
	}

	if registerRequest.GetWorkflowExecutionRetentionPeriodInDays() > int32(wh.config.DomainConfig.MaxRetentionDays()) {
		return validate.ErrInvalidRetention
	}

	if err := validate.CheckPermission(wh.config, registerRequest.SecurityToken); err != nil {
		return err
	}

	if err := checkRequiredDomainDataKVs(wh.config.DomainConfig.RequiredDomainDataKeys(), registerRequest.GetData()); err != nil {
		return err
	}

	if registerRequest.GetName() == "" {
		return validate.ErrDomainNotSet
	}

	return wh.domainHandler.RegisterDomain(ctx, registerRequest)
}

// ListDomains returns the information and configuration for a registered domain.
func (wh *WorkflowHandler) ListDomains(
	ctx context.Context,
	listRequest *types.ListDomainsRequest,
) (response *types.ListDomainsResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if listRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	return wh.domainHandler.ListDomains(ctx, listRequest)
}

// DescribeDomain returns the information and configuration for a registered domain.
func (wh *WorkflowHandler) DescribeDomain(
	ctx context.Context,
	describeRequest *types.DescribeDomainRequest,
) (response *types.DescribeDomainResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if describeRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	if describeRequest.GetName() == "" && describeRequest.GetUUID() == "" {
		return nil, validate.ErrDomainNotSet
	}

	resp, err := wh.domainHandler.DescribeDomain(ctx, describeRequest)
	if err != nil {
		return nil, err
	}

	if resp.GetFailoverInfo() != nil && resp.GetFailoverInfo().GetFailoverExpireTimestamp() > 0 {
		// fetch ongoing failover info from history service
		failoverResp, err := wh.GetHistoryClient().GetFailoverInfo(ctx, &types.GetFailoverInfoRequest{
			DomainID: resp.GetDomainInfo().UUID,
		})
		if err != nil {
			// despite the error from history, return describe domain response
			wh.GetLogger().Error(
				fmt.Sprintf("Failed to get failover info for domain %s", resp.DomainInfo.GetName()),
				tag.Error(err),
			)
			return resp, nil
		}
		resp.FailoverInfo.CompletedShardCount = failoverResp.GetCompletedShardCount()
		resp.FailoverInfo.PendingShards = failoverResp.GetPendingShards()
	}
	return resp, nil
}

// UpdateDomain is used to update the information and configuration for a registered domain.
func (wh *WorkflowHandler) UpdateDomain(
	ctx context.Context,
	updateRequest *types.UpdateDomainRequest,
) (resp *types.UpdateDomainResponse, retError error) {
	domainName := ""
	if updateRequest != nil {
		domainName = updateRequest.GetName()
	}

	logger := wh.GetLogger().WithTags(
		tag.WorkflowDomainName(domainName),
		tag.OperationName("DomainUpdate"))

	if updateRequest == nil {
		logger.Error("Nil domain update request.",
			tag.Error(validate.ErrRequestNotSet))
		return nil, validate.ErrRequestNotSet
	}

	isFailover := isFailoverRequest(updateRequest)
	isGraceFailover := isGraceFailoverRequest(updateRequest)
	logger.Info(fmt.Sprintf(
		"Domain Update requested. isFailover: %v, isGraceFailover: %v, Request: %#v.",
		isFailover,
		isGraceFailover,
		updateRequest))

	if wh.isShuttingDown() {
		logger.Error("Won't apply the domain update since workflowHandler is shutting down.",
			tag.Error(validate.ErrShuttingDown))
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		logger.Error("Won't apply the domain update since client version is not supported.",
			tag.Error(err))
		return nil, err
	}

	// don't require permission for failover request
	if isFailover {
		// reject the failover if the cluster is in lockdown
		if err := checkFailOverPermission(wh.config, updateRequest.GetName()); err != nil {
			logger.Error("Domain failover request rejected since domain is in lockdown.",
				tag.Error(err))
			return nil, err
		}
	} else {
		if err := validate.CheckPermission(wh.config, updateRequest.SecurityToken); err != nil {
			logger.Error("Domain update request rejected due to failing permissions.",
				tag.Error(err))
			return nil, err
		}
	}

	if isGraceFailover {
		if err := wh.checkOngoingFailover(
			ctx,
			&updateRequest.Name,
		); err != nil {
			logger.Error("Graceful domain failover request failed. Not able to check ongoing failovers.",
				tag.Error(err))
			return nil, err
		}
	}

	if updateRequest.GetName() == "" {
		logger.Error("Domain not set on request.",
			tag.Error(validate.ErrDomainNotSet))
		return nil, validate.ErrDomainNotSet
	}
	// TODO: call remote clusters to verify domain data
	resp, err := wh.domainHandler.UpdateDomain(ctx, updateRequest)
	if err != nil {
		logger.Error("Domain update operation failed.",
			tag.Error(err))
		return nil, err
	}
	logger.Info("Domain update operation succeeded.")
	return resp, nil
}

// DeprecateDomain us used to update status of a registered domain to DEPRECATED. Once the domain is deprecated
// it cannot be used to start new workflow executions.  Existing workflow executions will continue to run on
// deprecated domains.
func (wh *WorkflowHandler) DeprecateDomain(ctx context.Context, deprecateRequest *types.DeprecateDomainRequest) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return err
	}

	if deprecateRequest == nil {
		return validate.ErrRequestNotSet
	}

	if err := validate.CheckPermission(wh.config, deprecateRequest.SecurityToken); err != nil {
		return err
	}

	if deprecateRequest.GetName() == "" {
		return validate.ErrDomainNotSet
	}

	return wh.domainHandler.DeprecateDomain(ctx, deprecateRequest)
}

// PollForActivityTask - Poll for an activity task.
func (wh *WorkflowHandler) PollForActivityTask(
	ctx context.Context,
	pollRequest *types.PollForActivityTaskRequest,
) (resp *types.PollForActivityTaskResponse, retError error) {
	callTime := time.Now()

	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if pollRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	domainName := pollRequest.GetDomain()
	if domainName == "" {
		return nil, validate.ErrDomainNotSet
	}

	scope := getMetricsScopeWithDomain(metrics.FrontendPollForActivityTaskScope, pollRequest, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	wh.GetLogger().Debug("Received PollForActivityTask")
	if err := common.ValidateLongPollContextTimeout(
		ctx,
		"PollForActivityTask",
		wh.GetThrottledLogger().WithTags(tag.WorkflowDomainName(domainName), tag.WorkflowTaskListName(pollRequest.GetTaskList().GetName())),
	); err != nil {
		return nil, err
	}

	idLengthWarnLimit := wh.config.MaxIDLengthWarnLimit()
	if !common.IsValidIDLength(
		domainName,
		scope,
		idLengthWarnLimit,
		wh.config.DomainNameMaxLength(domainName),
		metrics.CadenceErrDomainNameExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeDomainName) {
		return nil, validate.ErrDomainTooLong
	}

	if err := wh.validateTaskList(pollRequest.TaskList, scope, domainName); err != nil {
		return nil, err
	}

	if !common.IsValidIDLength(
		pollRequest.GetIdentity(),
		scope,
		idLengthWarnLimit,
		wh.config.IdentityMaxLength(domainName),
		metrics.CadenceErrIdentityExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeIdentity) {
		return nil, validate.ErrIdentityTooLong
	}

	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return nil, err
	}

	isolationGroup := wh.getIsolationGroup(ctx, domainName)
	if !wh.waitUntilIsolationGroupHealthy(ctx, domainName, isolationGroup) {
		return &types.PollForActivityTaskResponse{}, nil
	}
	// it is possible that we wait for a very long time and the remaining time is not long enough for a long poll
	// in this case, return an empty response
	if err := common.ValidateLongPollContextTimeout(
		ctx,
		"PollForActivityTask",
		wh.GetThrottledLogger().WithTags(tag.WorkflowDomainName(domainName), tag.WorkflowTaskListName(pollRequest.GetTaskList().GetName())),
	); err != nil {
		return &types.PollForActivityTaskResponse{}, nil
	}
	pollerID := uuid.New().String()
	op := func() error {
		resp, err = wh.GetMatchingClient().PollForActivityTask(ctx, &types.MatchingPollForActivityTaskRequest{
			DomainUUID:     domainID,
			PollerID:       pollerID,
			PollRequest:    pollRequest,
			IsolationGroup: isolationGroup,
		})
		return err
	}

	err = wh.throttleRetry.Do(ctx, op)
	if err != nil {
		err = wh.cancelOutstandingPoll(ctx, err, domainID, persistence.TaskListTypeActivity, pollRequest.TaskList, pollerID)
		if err != nil {
			// For all other errors log an error and return it back to client.
			ctxTimeout := "not-set"
			ctxDeadline, ok := ctx.Deadline()
			if ok {
				ctxTimeout = ctxDeadline.Sub(callTime).String()
			}
			wh.GetLogger().Error("PollForActivityTask failed.",
				tag.WorkflowTaskListName(pollRequest.GetTaskList().GetName()),
				tag.Value(ctxTimeout),
				tag.Error(err))
			return nil, err
		}
	}
	return resp, nil
}

// PollForDecisionTask - Poll for a decision task.
func (wh *WorkflowHandler) PollForDecisionTask(
	ctx context.Context,
	pollRequest *types.PollForDecisionTaskRequest,
) (resp *types.PollForDecisionTaskResponse, retError error) {
	callTime := time.Now()

	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if pollRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	domainName := pollRequest.GetDomain()
	tags := getDomainWfIDRunIDTags(domainName, nil)

	if domainName == "" {
		return nil, validate.ErrDomainNotSet
	}

	scope := getMetricsScopeWithDomain(metrics.FrontendPollForDecisionTaskScope, pollRequest, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	wh.GetLogger().Debug("Received PollForDecisionTask")
	if err := common.ValidateLongPollContextTimeout(
		ctx,
		"PollForDecisionTask",
		wh.GetThrottledLogger().WithTags(tag.WorkflowDomainName(domainName), tag.WorkflowTaskListName(pollRequest.GetTaskList().GetName())),
	); err != nil {
		return nil, err
	}

	idLengthWarnLimit := wh.config.MaxIDLengthWarnLimit()
	if !common.IsValidIDLength(
		domainName,
		scope,
		idLengthWarnLimit,
		wh.config.DomainNameMaxLength(domainName),
		metrics.CadenceErrDomainNameExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeDomainName) {
		return nil, validate.ErrDomainTooLong
	}

	if !common.IsValidIDLength(
		pollRequest.GetIdentity(),
		scope,
		idLengthWarnLimit,
		wh.config.IdentityMaxLength(domainName),
		metrics.CadenceErrIdentityExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeIdentity) {
		return nil, validate.ErrIdentityTooLong
	}

	if err := wh.validateTaskList(pollRequest.TaskList, scope, domainName); err != nil {
		return nil, err
	}

	domainEntry, err := wh.GetDomainCache().GetDomain(domainName)
	if err != nil {
		return nil, err
	}
	domainID := domainEntry.GetInfo().ID

	wh.GetLogger().Debug("Poll for decision.", tag.WorkflowDomainName(domainName), tag.WorkflowDomainID(domainID))
	if err := wh.checkBadBinary(domainEntry, pollRequest.GetBinaryChecksum()); err != nil {
		return nil, err
	}

	isolationGroup := wh.getIsolationGroup(ctx, domainName)
	if !wh.waitUntilIsolationGroupHealthy(ctx, domainName, isolationGroup) {
		return &types.PollForDecisionTaskResponse{}, nil
	}
	// it is possible that we wait for a very long time and the remaining time is not long enough for a long poll
	// in this case, return an empty response
	if err := common.ValidateLongPollContextTimeout(
		ctx,
		"PollForDecisionTask",
		wh.GetThrottledLogger().WithTags(tag.WorkflowDomainName(domainName), tag.WorkflowTaskListName(pollRequest.GetTaskList().GetName())),
	); err != nil {
		return &types.PollForDecisionTaskResponse{}, nil
	}

	pollerID := uuid.New().String()
	var matchingResp *types.MatchingPollForDecisionTaskResponse
	op := func() error {
		matchingResp, err = wh.GetMatchingClient().PollForDecisionTask(ctx, &types.MatchingPollForDecisionTaskRequest{
			DomainUUID:     domainID,
			PollerID:       pollerID,
			PollRequest:    pollRequest,
			IsolationGroup: isolationGroup,
		})
		return err
	}

	err = wh.throttleRetry.Do(ctx, op)
	if err != nil {
		err = wh.cancelOutstandingPoll(ctx, err, domainID, persistence.TaskListTypeDecision, pollRequest.TaskList, pollerID)
		if err != nil {
			// For all other errors log an error and return it back to client.
			ctxTimeout := "not-set"
			ctxDeadline, ok := ctx.Deadline()
			if ok {
				ctxTimeout = ctxDeadline.Sub(callTime).String()
			}
			wh.GetLogger().Error("PollForDecisionTask failed.",
				tag.WorkflowTaskListName(pollRequest.GetTaskList().GetName()),
				tag.Value(ctxTimeout),
				tag.Error(err))
			return nil, err
		}

		// Must be cancellation error.  Does'nt matter what we return here.  Client already went away.
		return nil, nil
	}

	tags = append(tags, []tag.Tag{tag.WorkflowID(
		matchingResp.GetWorkflowExecution().GetWorkflowID()),
		tag.WorkflowRunID(matchingResp.GetWorkflowExecution().GetRunID())}...)
	resp, err = wh.createPollForDecisionTaskResponse(ctx, scope, domainID, matchingResp, matchingResp.GetBranchToken())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (wh *WorkflowHandler) getIsolationGroup(ctx context.Context, domainName string) string {
	return partition.IsolationGroupFromContext(ctx)
}

func (wh *WorkflowHandler) getPartitionConfig(ctx context.Context, domainName string) map[string]string {
	return partition.ConfigFromContext(ctx)
}

func (wh *WorkflowHandler) isIsolationGroupHealthy(ctx context.Context, domainName, isolationGroup string) bool {
	if wh.GetIsolationGroupState() != nil && wh.config.EnableTasklistIsolation(domainName) {
		isDrained, err := wh.GetIsolationGroupState().IsDrained(ctx, domainName, isolationGroup)
		if err != nil {
			wh.GetLogger().Error("Failed to check if an isolation group is drained, assume it's healthy", tag.Error(err))
			return true
		}
		return !isDrained
	}
	return true
}

func (wh *WorkflowHandler) waitUntilIsolationGroupHealthy(ctx context.Context, domainName, isolationGroup string) bool {
	if wh.GetIsolationGroupState() != nil && wh.config.EnableTasklistIsolation(domainName) {
		ticker := time.NewTicker(time.Second * 30)
		defer ticker.Stop()
		childCtx, cancel := common.CreateChildContext(ctx, 0.05)
		defer cancel()
		for {
			isDrained, err := wh.GetIsolationGroupState().IsDrained(childCtx, domainName, isolationGroup)
			if err != nil {
				wh.GetLogger().Error("Failed to check if an isolation group is drained, assume it's healthy", tag.Error(err))
				return true
			}
			if !isDrained {
				break
			}
			select {
			case <-childCtx.Done():
				return false
			case <-ticker.C:
			}
		}
	}
	return true
}

func (wh *WorkflowHandler) checkBadBinary(domainEntry *cache.DomainCacheEntry, binaryChecksum string) error {
	if domainEntry.GetConfig().BadBinaries.Binaries != nil {
		badBinaries := domainEntry.GetConfig().BadBinaries.Binaries
		_, ok := badBinaries[binaryChecksum]
		if ok {
			wh.GetMetricsClient().IncCounter(metrics.FrontendPollForDecisionTaskScope, metrics.CadenceErrBadBinaryCounter)
			return &types.BadRequestError{
				Message: fmt.Sprintf("binary %v already marked as bad deployment", binaryChecksum),
			}
		}
	}
	return nil
}

func (wh *WorkflowHandler) cancelOutstandingPoll(ctx context.Context, err error, domainID string, taskListType int32,
	taskList *types.TaskList, pollerID string) error {
	// First check if this err is due to context cancellation.  This means client connection to frontend is closed.
	if ctx.Err() == context.Canceled {
		// Our rpc stack does not propagates context cancellation to the other service.  Lets make an explicit
		// call to matching to notify this poller is gone to prevent any tasks being dispatched to zombie pollers.
		err = wh.GetMatchingClient().CancelOutstandingPoll(context.Background(), &types.CancelOutstandingPollRequest{
			DomainUUID:   domainID,
			TaskListType: common.Int32Ptr(taskListType),
			TaskList:     taskList,
			PollerID:     pollerID,
		})
		// We can not do much if this call fails.  Just log the error and move on
		if err != nil {
			wh.GetLogger().Warn("Failed to cancel outstanding poller.",
				tag.WorkflowTaskListName(taskList.GetName()), tag.Error(err))
		}

		// Clear error as we don't want to report context cancellation error to count against our SLA
		return nil
	}

	return err
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (wh *WorkflowHandler) RecordActivityTaskHeartbeat(
	ctx context.Context,
	heartbeatRequest *types.RecordActivityTaskHeartbeatRequest,
) (resp *types.RecordActivityTaskHeartbeatResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if heartbeatRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	wh.GetLogger().Debug("Received RecordActivityTaskHeartbeat")
	if heartbeatRequest.TaskToken == nil {
		return nil, validate.ErrTaskTokenNotSet
	}
	taskToken, err := wh.tokenSerializer.Deserialize(heartbeatRequest.TaskToken)
	if err != nil {
		return nil, err
	}
	if taskToken.DomainID == "" {
		return nil, validate.ErrDomainNotSet
	}

	domainName, err := wh.GetDomainCache().GetDomainName(taskToken.DomainID)
	if err != nil {
		return nil, err
	}

	dw := domainWrapper{
		domain: domainName,
	}
	scope := getMetricsScopeWithDomain(metrics.FrontendRecordActivityTaskHeartbeatScope, dw, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)

	if err := common.CheckEventBlobSizeLimit(
		len(heartbeatRequest.Details),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
		tag.BlobSizeViolationOperation("RecordActivityTaskHeartbeat"),
	); err != nil {
		// heartbeat details exceed size limit, we would fail the activity immediately with explicit error reason
		failRequest := &types.RespondActivityTaskFailedRequest{
			TaskToken: heartbeatRequest.TaskToken,
			Reason:    common.StringPtr(common.FailureReasonHeartbeatExceedsLimit),
			Details:   heartbeatRequest.Details[0:sizeLimitError],
			Identity:  heartbeatRequest.Identity,
		}
		err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &types.HistoryRespondActivityTaskFailedRequest{
			DomainUUID:    taskToken.DomainID,
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, wh.normalizeVersionedErrors(ctx, err)
		}
		resp = &types.RecordActivityTaskHeartbeatResponse{CancelRequested: true}
	} else {
		resp, err = wh.GetHistoryClient().RecordActivityTaskHeartbeat(ctx, &types.HistoryRecordActivityTaskHeartbeatRequest{
			DomainUUID:       taskToken.DomainID,
			HeartbeatRequest: heartbeatRequest,
		})
		if err != nil {
			return nil, wh.normalizeVersionedErrors(ctx, err)
		}
	}

	return resp, nil
}

// RecordActivityTaskHeartbeatByID - Record Activity Task Heart beat.
func (wh *WorkflowHandler) RecordActivityTaskHeartbeatByID(
	ctx context.Context,
	heartbeatRequest *types.RecordActivityTaskHeartbeatByIDRequest,
) (resp *types.RecordActivityTaskHeartbeatResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if heartbeatRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	domainName := heartbeatRequest.GetDomain()
	if domainName == "" {
		return nil, validate.ErrDomainNotSet
	}

	wh.GetLogger().Debug("Received RecordActivityTaskHeartbeatByID")
	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return nil, err
	}
	workflowID := heartbeatRequest.GetWorkflowID()
	runID := heartbeatRequest.GetRunID() // runID is optional so can be empty
	activityID := heartbeatRequest.GetActivityID()

	if domainID == "" {
		return nil, validate.ErrDomainNotSet
	}
	if workflowID == "" {
		return nil, validate.ErrWorkflowIDNotSet
	}
	if activityID == "" {
		return nil, validate.ErrActivityIDNotSet
	}

	taskToken := &common.TaskToken{
		DomainID:   domainID,
		RunID:      runID,
		WorkflowID: workflowID,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	}
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, err
	}

	scope := getMetricsScopeWithDomain(metrics.FrontendRecordActivityTaskHeartbeatByIDScope, heartbeatRequest, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)

	if err := common.CheckEventBlobSizeLimit(
		len(heartbeatRequest.Details),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
		tag.BlobSizeViolationOperation("RecordActivityTaskHeartbeatByID"),
	); err != nil {
		// heartbeat details exceed size limit, we would fail the activity immediately with explicit error reason
		failRequest := &types.RespondActivityTaskFailedRequest{
			TaskToken: token,
			Reason:    common.StringPtr(common.FailureReasonHeartbeatExceedsLimit),
			Details:   heartbeatRequest.Details[0:sizeLimitError],
			Identity:  heartbeatRequest.Identity,
		}
		err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &types.HistoryRespondActivityTaskFailedRequest{
			DomainUUID:    taskToken.DomainID,
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, wh.normalizeVersionedErrors(ctx, err)
		}
		resp = &types.RecordActivityTaskHeartbeatResponse{CancelRequested: true}
	} else {
		req := &types.RecordActivityTaskHeartbeatRequest{
			TaskToken: token,
			Details:   heartbeatRequest.Details,
			Identity:  heartbeatRequest.Identity,
		}

		resp, err = wh.GetHistoryClient().RecordActivityTaskHeartbeat(ctx, &types.HistoryRecordActivityTaskHeartbeatRequest{
			DomainUUID:       taskToken.DomainID,
			HeartbeatRequest: req,
		})
		if err != nil {
			return nil, wh.normalizeVersionedErrors(ctx, err)
		}
	}

	return resp, nil
}

// RespondActivityTaskCompleted - response to an activity task
func (wh *WorkflowHandler) RespondActivityTaskCompleted(
	ctx context.Context,
	completeRequest *types.RespondActivityTaskCompletedRequest,
) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return err
	}

	if completeRequest == nil {
		return validate.ErrRequestNotSet
	}

	if completeRequest.TaskToken == nil {
		return validate.ErrTaskTokenNotSet
	}
	taskToken, err := wh.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err != nil {
		return err
	}
	if taskToken.DomainID == "" {
		return validate.ErrDomainNotSet
	}

	domainName, err := wh.GetDomainCache().GetDomainName(taskToken.DomainID)
	if err != nil {
		return err
	}

	dw := domainWrapper{
		domain: domainName,
	}
	scope := getMetricsScopeWithDomain(metrics.FrontendRespondActivityTaskCompletedScope, dw, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	if !common.IsValidIDLength(
		completeRequest.GetIdentity(),
		scope,
		wh.config.MaxIDLengthWarnLimit(),
		wh.config.IdentityMaxLength(domainName),
		metrics.CadenceErrIdentityExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeIdentity) {
		return validate.ErrIdentityTooLong
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)

	if err := common.CheckEventBlobSizeLimit(
		len(completeRequest.Result),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
		tag.BlobSizeViolationOperation("RespondActivityTaskCompleted"),
	); err != nil {
		// result exceeds blob size limit, we would record it as failure
		failRequest := &types.RespondActivityTaskFailedRequest{
			TaskToken: completeRequest.TaskToken,
			Reason:    common.StringPtr(common.FailureReasonCompleteResultExceedsLimit),
			Details:   completeRequest.Result[0:sizeLimitError],
			Identity:  completeRequest.Identity,
		}
		err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &types.HistoryRespondActivityTaskFailedRequest{
			DomainUUID:    taskToken.DomainID,
			FailedRequest: failRequest,
		})
		if err != nil {
			return wh.normalizeVersionedErrors(ctx, err)
		}
	} else {
		err = wh.GetHistoryClient().RespondActivityTaskCompleted(ctx, &types.HistoryRespondActivityTaskCompletedRequest{
			DomainUUID:      taskToken.DomainID,
			CompleteRequest: completeRequest,
		})
		if err != nil {
			return wh.normalizeVersionedErrors(ctx, err)
		}
	}

	return nil
}

// RespondActivityTaskCompletedByID - response to an activity task
func (wh *WorkflowHandler) RespondActivityTaskCompletedByID(
	ctx context.Context,
	completeRequest *types.RespondActivityTaskCompletedByIDRequest,
) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return err
	}

	if completeRequest == nil {
		return validate.ErrRequestNotSet
	}

	domainName := completeRequest.GetDomain()
	if domainName == "" {
		return validate.ErrDomainNotSet
	}
	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return err
	}
	workflowID := completeRequest.GetWorkflowID()
	runID := completeRequest.GetRunID() // runID is optional so can be empty
	activityID := completeRequest.GetActivityID()

	if domainID == "" {
		return validate.ErrDomainNotSet
	}
	if workflowID == "" {
		return validate.ErrWorkflowIDNotSet
	}
	if activityID == "" {
		return validate.ErrActivityIDNotSet
	}

	scope := getMetricsScopeWithDomain(metrics.FrontendRespondActivityTaskCompletedByIDScope, completeRequest, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	if !common.IsValidIDLength(
		completeRequest.GetIdentity(),
		scope,
		wh.config.MaxIDLengthWarnLimit(),
		wh.config.IdentityMaxLength(domainName),
		metrics.CadenceErrIdentityExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeIdentity) {
		return validate.ErrIdentityTooLong
	}

	taskToken := &common.TaskToken{
		DomainID:   domainID,
		RunID:      runID,
		WorkflowID: workflowID,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	}
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)

	if err := common.CheckEventBlobSizeLimit(
		len(completeRequest.Result),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
		tag.BlobSizeViolationOperation("RespondActivityTaskCompletedByID"),
	); err != nil {
		// result exceeds blob size limit, we would record it as failure
		failRequest := &types.RespondActivityTaskFailedRequest{
			TaskToken: token,
			Reason:    common.StringPtr(common.FailureReasonCompleteResultExceedsLimit),
			Details:   completeRequest.Result[0:sizeLimitError],
			Identity:  completeRequest.Identity,
		}
		err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &types.HistoryRespondActivityTaskFailedRequest{
			DomainUUID:    taskToken.DomainID,
			FailedRequest: failRequest,
		})
		if err != nil {
			return wh.normalizeVersionedErrors(ctx, err)
		}
	} else {
		req := &types.RespondActivityTaskCompletedRequest{
			TaskToken: token,
			Result:    completeRequest.Result,
			Identity:  completeRequest.Identity,
		}

		err = wh.GetHistoryClient().RespondActivityTaskCompleted(ctx, &types.HistoryRespondActivityTaskCompletedRequest{
			DomainUUID:      taskToken.DomainID,
			CompleteRequest: req,
		})
		if err != nil {
			return wh.normalizeVersionedErrors(ctx, err)
		}
	}

	return nil
}

// RespondActivityTaskFailed - response to an activity task failure
func (wh *WorkflowHandler) RespondActivityTaskFailed(
	ctx context.Context,
	failedRequest *types.RespondActivityTaskFailedRequest,
) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return err
	}

	if failedRequest == nil {
		return validate.ErrRequestNotSet
	}

	if failedRequest.TaskToken == nil {
		return validate.ErrTaskTokenNotSet
	}
	taskToken, err := wh.tokenSerializer.Deserialize(failedRequest.TaskToken)
	if err != nil {
		return err
	}
	if taskToken.DomainID == "" {
		return validate.ErrDomainNotSet
	}

	domainName, err := wh.GetDomainCache().GetDomainName(taskToken.DomainID)
	if err != nil {
		return err
	}

	dw := domainWrapper{
		domain: domainName,
	}
	scope := getMetricsScopeWithDomain(metrics.FrontendRespondActivityTaskFailedScope, dw, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	if !common.IsValidIDLength(
		failedRequest.GetIdentity(),
		scope,
		wh.config.MaxIDLengthWarnLimit(),
		wh.config.IdentityMaxLength(domainName),
		metrics.CadenceErrIdentityExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeIdentity) {
		return validate.ErrIdentityTooLong
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)

	if err := common.CheckEventBlobSizeLimit(
		len(failedRequest.Details),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
		tag.BlobSizeViolationOperation("RespondActivityTaskFailed"),
	); err != nil {
		// details exceeds blob size limit, we would truncate the details and put a specific error reason
		failedRequest.Reason = common.StringPtr(common.FailureReasonFailureDetailsExceedsLimit)
		failedRequest.Details = failedRequest.Details[0:sizeLimitError]
	}

	err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &types.HistoryRespondActivityTaskFailedRequest{
		DomainUUID:    taskToken.DomainID,
		FailedRequest: failedRequest,
	})
	if err != nil {
		return wh.normalizeVersionedErrors(ctx, err)
	}
	return nil
}

// RespondActivityTaskFailedByID - response to an activity task failure
func (wh *WorkflowHandler) RespondActivityTaskFailedByID(
	ctx context.Context,
	failedRequest *types.RespondActivityTaskFailedByIDRequest,
) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return err
	}

	if failedRequest == nil {
		return validate.ErrRequestNotSet
	}

	domainName := failedRequest.GetDomain()

	if domainName == "" {
		return validate.ErrDomainNotSet
	}
	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return err
	}
	workflowID := failedRequest.GetWorkflowID()
	runID := failedRequest.GetRunID() // runID is optional so can be empty
	activityID := failedRequest.GetActivityID()

	if domainID == "" {
		return validate.ErrDomainNotSet
	}
	if workflowID == "" {
		return validate.ErrWorkflowIDNotSet
	}
	if activityID == "" {
		return validate.ErrActivityIDNotSet
	}

	scope := getMetricsScopeWithDomain(metrics.FrontendRespondActivityTaskFailedByIDScope, failedRequest, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	if !common.IsValidIDLength(
		failedRequest.GetIdentity(),
		scope,
		wh.config.MaxIDLengthWarnLimit(),
		wh.config.IdentityMaxLength(failedRequest.GetDomain()),
		metrics.CadenceErrIdentityExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeIdentity) {
		return validate.ErrIdentityTooLong
	}

	taskToken := &common.TaskToken{
		DomainID:   domainID,
		RunID:      runID,
		WorkflowID: workflowID,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	}
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)

	if err := common.CheckEventBlobSizeLimit(
		len(failedRequest.Details),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
		tag.BlobSizeViolationOperation("RespondActivityTaskFailedByID"),
	); err != nil {
		// details exceeds blob size limit, we would truncate the details and put a specific error reason
		failedRequest.Reason = common.StringPtr(common.FailureReasonFailureDetailsExceedsLimit)
		failedRequest.Details = failedRequest.Details[0:sizeLimitError]
	}

	req := &types.RespondActivityTaskFailedRequest{
		TaskToken: token,
		Reason:    failedRequest.Reason,
		Details:   failedRequest.Details,
		Identity:  failedRequest.Identity,
	}

	err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &types.HistoryRespondActivityTaskFailedRequest{
		DomainUUID:    taskToken.DomainID,
		FailedRequest: req,
	})
	if err != nil {
		return wh.normalizeVersionedErrors(ctx, err)
	}
	return nil
}

// RespondActivityTaskCanceled - called to cancel an activity task
func (wh *WorkflowHandler) RespondActivityTaskCanceled(
	ctx context.Context,
	cancelRequest *types.RespondActivityTaskCanceledRequest,
) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return err
	}

	if cancelRequest == nil {
		return validate.ErrRequestNotSet
	}

	if cancelRequest.TaskToken == nil {
		return validate.ErrTaskTokenNotSet
	}

	taskToken, err := wh.tokenSerializer.Deserialize(cancelRequest.TaskToken)
	if err != nil {
		return err
	}

	if taskToken.DomainID == "" {
		return validate.ErrDomainNotSet
	}

	domainName, err := wh.GetDomainCache().GetDomainName(taskToken.DomainID)
	if err != nil {
		return err
	}

	dw := domainWrapper{
		domain: domainName,
	}
	scope := getMetricsScopeWithDomain(metrics.FrontendRespondActivityTaskCanceledScope, dw, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	if !common.IsValidIDLength(
		cancelRequest.GetIdentity(),
		scope,
		wh.config.MaxIDLengthWarnLimit(),
		wh.config.IdentityMaxLength(domainName),
		metrics.CadenceErrIdentityExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeIdentity) {
		return validate.ErrIdentityTooLong
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)

	if err := common.CheckEventBlobSizeLimit(
		len(cancelRequest.Details),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
		tag.BlobSizeViolationOperation("RespondActivityTaskCanceled"),
	); err != nil {
		// details exceeds blob size limit, we would record it as failure
		failRequest := &types.RespondActivityTaskFailedRequest{
			TaskToken: cancelRequest.TaskToken,
			Reason:    common.StringPtr(common.FailureReasonCancelDetailsExceedsLimit),
			Details:   cancelRequest.Details[0:sizeLimitError],
			Identity:  cancelRequest.Identity,
		}
		err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &types.HistoryRespondActivityTaskFailedRequest{
			DomainUUID:    taskToken.DomainID,
			FailedRequest: failRequest,
		})
		if err != nil {
			return wh.normalizeVersionedErrors(ctx, err)
		}
	} else {
		err = wh.GetHistoryClient().RespondActivityTaskCanceled(ctx, &types.HistoryRespondActivityTaskCanceledRequest{
			DomainUUID:    taskToken.DomainID,
			CancelRequest: cancelRequest,
		})
		if err != nil {
			return wh.normalizeVersionedErrors(ctx, err)
		}
	}

	return nil
}

// RespondActivityTaskCanceledByID - called to cancel an activity task
func (wh *WorkflowHandler) RespondActivityTaskCanceledByID(
	ctx context.Context,
	cancelRequest *types.RespondActivityTaskCanceledByIDRequest,
) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return err
	}

	if cancelRequest == nil {
		return validate.ErrRequestNotSet
	}

	domainName := cancelRequest.GetDomain()
	if domainName == "" {
		return validate.ErrDomainNotSet
	}
	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return err
	}
	workflowID := cancelRequest.GetWorkflowID()
	runID := cancelRequest.GetRunID() // runID is optional so can be empty
	activityID := cancelRequest.GetActivityID()

	if domainID == "" {
		return validate.ErrDomainNotSet
	}
	if workflowID == "" {
		return validate.ErrWorkflowIDNotSet
	}
	if activityID == "" {
		return validate.ErrActivityIDNotSet
	}

	scope := getMetricsScopeWithDomain(metrics.FrontendRespondActivityTaskCanceledByIDScope, cancelRequest, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	if !common.IsValidIDLength(
		cancelRequest.GetIdentity(),
		scope,
		wh.config.MaxIDLengthWarnLimit(),
		wh.config.IdentityMaxLength(cancelRequest.GetDomain()),
		metrics.CadenceErrIdentityExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeIdentity) {
		return validate.ErrIdentityTooLong
	}

	taskToken := &common.TaskToken{
		DomainID:   domainID,
		RunID:      runID,
		WorkflowID: workflowID,
		ScheduleID: common.EmptyEventID,
		ActivityID: activityID,
	}
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)

	if err := common.CheckEventBlobSizeLimit(
		len(cancelRequest.Details),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
		tag.BlobSizeViolationOperation("RespondActivityTaskCanceledByID"),
	); err != nil {
		// details exceeds blob size limit, we would record it as failure
		failRequest := &types.RespondActivityTaskFailedRequest{
			TaskToken: token,
			Reason:    common.StringPtr(common.FailureReasonCancelDetailsExceedsLimit),
			Details:   cancelRequest.Details[0:sizeLimitError],
			Identity:  cancelRequest.Identity,
		}
		err = wh.GetHistoryClient().RespondActivityTaskFailed(ctx, &types.HistoryRespondActivityTaskFailedRequest{
			DomainUUID:    taskToken.DomainID,
			FailedRequest: failRequest,
		})
		if err != nil {
			return wh.normalizeVersionedErrors(ctx, err)
		}
	} else {
		req := &types.RespondActivityTaskCanceledRequest{
			TaskToken: token,
			Details:   cancelRequest.Details,
			Identity:  cancelRequest.Identity,
		}

		err = wh.GetHistoryClient().RespondActivityTaskCanceled(ctx, &types.HistoryRespondActivityTaskCanceledRequest{
			DomainUUID:    taskToken.DomainID,
			CancelRequest: req,
		})
		if err != nil {
			return wh.normalizeVersionedErrors(ctx, err)
		}
	}

	return nil
}

// RespondDecisionTaskCompleted - response to a decision task
func (wh *WorkflowHandler) RespondDecisionTaskCompleted(
	ctx context.Context,
	completeRequest *types.RespondDecisionTaskCompletedRequest,
) (resp *types.RespondDecisionTaskCompletedResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if completeRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	if completeRequest.TaskToken == nil {
		return nil, validate.ErrTaskTokenNotSet
	}
	taskToken, err := wh.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err != nil {
		return nil, err
	}
	if taskToken.DomainID == "" {
		return nil, validate.ErrDomainNotSet
	}

	domainName, err := wh.GetDomainCache().GetDomainName(taskToken.DomainID)
	if err != nil {
		return nil, err
	}

	dw := domainWrapper{
		domain: domainName,
	}
	scope := getMetricsScopeWithDomain(metrics.FrontendRespondDecisionTaskCompletedScope, dw, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	if !common.IsValidIDLength(
		completeRequest.GetIdentity(),
		scope,
		wh.config.MaxIDLengthWarnLimit(),
		wh.config.IdentityMaxLength(domainName),
		metrics.CadenceErrIdentityExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeIdentity) {
		return nil, validate.ErrIdentityTooLong
	}

	if err := common.CheckDecisionResultLimit(
		len(completeRequest.Decisions),
		wh.config.DecisionResultCountLimit(domainName),
		scope); err != nil {
		return nil, err
	}

	histResp, err := wh.GetHistoryClient().RespondDecisionTaskCompleted(ctx, &types.HistoryRespondDecisionTaskCompletedRequest{
		DomainUUID:      taskToken.DomainID,
		CompleteRequest: completeRequest},
	)
	if err != nil {
		return nil, wh.normalizeVersionedErrors(ctx, err)
	}

	completedResp := &types.RespondDecisionTaskCompletedResponse{}
	completedResp.ActivitiesToDispatchLocally = histResp.ActivitiesToDispatchLocally
	if completeRequest.GetReturnNewDecisionTask() && histResp != nil && histResp.StartedResponse != nil {
		taskToken := &common.TaskToken{
			DomainID:        taskToken.DomainID,
			WorkflowID:      taskToken.WorkflowID,
			RunID:           taskToken.RunID,
			ScheduleID:      histResp.StartedResponse.GetScheduledEventID(),
			ScheduleAttempt: histResp.StartedResponse.GetAttempt(),
		}
		token, _ := wh.tokenSerializer.Serialize(taskToken)
		workflowExecution := &types.WorkflowExecution{
			WorkflowID: taskToken.WorkflowID,
			RunID:      taskToken.RunID,
		}
		matchingResp := common.CreateMatchingPollForDecisionTaskResponse(histResp.StartedResponse, workflowExecution, token)

		newDecisionTask, err := wh.createPollForDecisionTaskResponse(ctx, scope, taskToken.DomainID, matchingResp, matchingResp.GetBranchToken())
		if err != nil {
			return nil, err
		}
		completedResp.DecisionTask = newDecisionTask
	}

	return completedResp, nil
}

// RespondDecisionTaskFailed - failed response to a decision task
func (wh *WorkflowHandler) RespondDecisionTaskFailed(
	ctx context.Context,
	failedRequest *types.RespondDecisionTaskFailedRequest,
) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return err
	}

	if failedRequest == nil {
		return validate.ErrRequestNotSet
	}

	if failedRequest.TaskToken == nil {
		return validate.ErrTaskTokenNotSet
	}
	taskToken, err := wh.tokenSerializer.Deserialize(failedRequest.TaskToken)
	if err != nil {
		return err
	}
	if taskToken.DomainID == "" {
		return validate.ErrDomainNotSet
	}

	domainName, err := wh.GetDomainCache().GetDomainName(taskToken.DomainID)
	if err != nil {
		return err
	}

	dw := domainWrapper{
		domain: domainName,
	}
	scope := getMetricsScopeWithDomain(metrics.FrontendRespondDecisionTaskFailedScope, dw, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	if !common.IsValidIDLength(
		failedRequest.GetIdentity(),
		scope,
		wh.config.MaxIDLengthWarnLimit(),
		wh.config.IdentityMaxLength(domainName),
		metrics.CadenceErrIdentityExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeIdentity) {
		return validate.ErrIdentityTooLong
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)

	if err := common.CheckEventBlobSizeLimit(
		len(failedRequest.Details),
		sizeLimitWarn,
		sizeLimitError,
		taskToken.DomainID,
		taskToken.WorkflowID,
		taskToken.RunID,
		scope,
		wh.GetThrottledLogger(),
		tag.BlobSizeViolationOperation("RespondDecisionTaskFailed"),
	); err != nil {
		// details exceed, we would just truncate the size for decision task failed as the details is not used anywhere by client code
		failedRequest.Details = failedRequest.Details[0:sizeLimitError]
	}

	err = wh.GetHistoryClient().RespondDecisionTaskFailed(ctx, &types.HistoryRespondDecisionTaskFailedRequest{
		DomainUUID:    taskToken.DomainID,
		FailedRequest: failedRequest,
	})
	if err != nil {
		return wh.normalizeVersionedErrors(ctx, err)
	}
	return nil
}

// RespondQueryTaskCompleted - response to a query task
func (wh *WorkflowHandler) RespondQueryTaskCompleted(
	ctx context.Context,
	completeRequest *types.RespondQueryTaskCompletedRequest,
) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return err
	}

	if completeRequest == nil {
		return validate.ErrRequestNotSet
	}

	if completeRequest.TaskToken == nil {
		return validate.ErrTaskTokenNotSet
	}
	queryTaskToken, err := wh.tokenSerializer.DeserializeQueryTaskToken(completeRequest.TaskToken)
	if err != nil {
		return err
	}
	if queryTaskToken.DomainID == "" || queryTaskToken.TaskList == "" || queryTaskToken.TaskID == "" {
		return validate.ErrInvalidTaskToken
	}

	domainName, err := wh.GetDomainCache().GetDomainName(queryTaskToken.DomainID)
	if err != nil {
		return err
	}

	dw := domainWrapper{
		domain: domainName,
	}
	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)

	scope := getMetricsScopeWithDomain(metrics.FrontendRespondQueryTaskCompletedScope, dw, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	if err := common.CheckEventBlobSizeLimit(
		len(completeRequest.GetQueryResult()),
		sizeLimitWarn,
		sizeLimitError,
		queryTaskToken.DomainID,
		"",
		"",
		scope,
		wh.GetThrottledLogger(),
		tag.BlobSizeViolationOperation("RespondQueryTaskCompleted"),
	); err != nil {
		completeRequest = &types.RespondQueryTaskCompletedRequest{
			TaskToken:     completeRequest.TaskToken,
			CompletedType: types.QueryTaskCompletedTypeFailed.Ptr(),
			QueryResult:   nil,
			ErrorMessage:  err.Error(),
		}
	}

	call := yarpc.CallFromContext(ctx)

	completeRequest.WorkerVersionInfo = &types.WorkerVersionInfo{
		Impl:           call.Header(common.ClientImplHeaderName),
		FeatureVersion: call.Header(common.FeatureVersionHeaderName),
	}
	matchingRequest := &types.MatchingRespondQueryTaskCompletedRequest{
		DomainUUID:       queryTaskToken.DomainID,
		TaskList:         &types.TaskList{Name: queryTaskToken.TaskList},
		TaskID:           queryTaskToken.TaskID,
		CompletedRequest: completeRequest,
	}

	err = wh.GetMatchingClient().RespondQueryTaskCompleted(ctx, matchingRequest)
	if err != nil {
		return err
	}
	return nil
}

func (wh *WorkflowHandler) StartWorkflowExecutionAsync(
	ctx context.Context,
	startRequest *types.StartWorkflowExecutionAsyncRequest,
) (resp *types.StartWorkflowExecutionAsyncResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}
	scope := getMetricsScopeWithDomain(metrics.FrontendStartWorkflowExecutionAsyncScope, startRequest, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	// validate request before pushing to queue
	err := wh.validateStartWorkflowExecutionRequest(ctx, startRequest.StartWorkflowExecutionRequest, scope)
	if err != nil {
		return nil, err
	}

	producer, err := wh.producerManager.GetProducerByDomain(startRequest.GetDomain())
	if err != nil {
		return nil, err
	}

	// Serialize the message to be sent to the queue.
	// Avoid JSON because json encoding of requests excludes PII fields such as input. JSON encoded request are logged by acccess controlled api layer for audit purposes.
	payload, err := wh.thriftrwEncoder.Encode(thrift.FromStartWorkflowExecutionAsyncRequest(startRequest))
	if err != nil {
		return nil, fmt.Errorf("failed to encode StartWorkflowExecutionAsyncRequest: %v", err)
	}
	scope.RecordTimer(metrics.AsyncRequestPayloadSize, time.Duration(len(payload)))

	// propagate the headers from the context to the message
	header := &shared.Header{
		Fields: make(map[string][]byte),
	}
	for k, v := range yarpc.CallFromContext(ctx).OriginalHeaders() {
		header.Fields[k] = []byte(v)
	}
	messageType := sqlblobs.AsyncRequestTypeStartWorkflowExecutionAsyncRequest
	message := &sqlblobs.AsyncRequestMessage{
		PartitionKey: common.StringPtr(startRequest.GetWorkflowID()),
		Type:         &messageType,
		Header:       header,
		Encoding:     common.StringPtr(string(common.EncodingTypeThriftRW)),
		Payload:      payload,
	}
	err = producer.Publish(ctx, message)
	if err != nil {
		return nil, err
	}
	return &types.StartWorkflowExecutionAsyncResponse{}, nil
}

// StartWorkflowExecution - Creates a new workflow execution
func (wh *WorkflowHandler) StartWorkflowExecution(
	ctx context.Context,
	startRequest *types.StartWorkflowExecutionRequest,
) (resp *types.StartWorkflowExecutionResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}
	scope := getMetricsScopeWithDomain(metrics.FrontendStartWorkflowExecutionScope, startRequest, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	err := wh.validateStartWorkflowExecutionRequest(ctx, startRequest, scope)
	if err != nil {
		return nil, err
	}
	domainName := startRequest.GetDomain()
	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return nil, err
	}
	wh.GetLogger().Debug("Start workflow execution request domainID", tag.WorkflowDomainID(domainID))
	historyRequest, err := common.CreateHistoryStartWorkflowRequest(
		domainID, startRequest, time.Now(), wh.getPartitionConfig(ctx, domainName))
	if err != nil {
		return nil, err
	}

	resp, err = wh.GetHistoryClient().StartWorkflowExecution(ctx, historyRequest)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (wh *WorkflowHandler) validateStartWorkflowExecutionRequest(ctx context.Context, startRequest *types.StartWorkflowExecutionRequest, scope metrics.Scope) error {
	if startRequest == nil {
		return validate.ErrRequestNotSet
	}
	domainName := startRequest.GetDomain()
	if domainName == "" {
		return validate.ErrDomainNotSet
	}
	if startRequest.GetWorkflowID() == "" {
		return validate.ErrWorkflowIDNotSet
	}
	if _, err := uuid.Parse(startRequest.RequestID); err != nil {
		return &types.BadRequestError{Message: fmt.Sprintf("requestId %q is not a valid UUID", startRequest.RequestID)}
	}
	if startRequest.WorkflowType == nil || startRequest.WorkflowType.GetName() == "" {
		return validate.ErrWorkflowTypeNotSet
	}
	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return err
	}
	idLengthWarnLimit := wh.config.MaxIDLengthWarnLimit()
	if !common.IsValidIDLength(
		domainName,
		scope,
		idLengthWarnLimit,
		wh.config.DomainNameMaxLength(domainName),
		metrics.CadenceErrDomainNameExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeDomainName) {
		return validate.ErrDomainTooLong
	}
	if !common.IsValidIDLength(
		startRequest.GetWorkflowID(),
		scope,
		idLengthWarnLimit,
		wh.config.WorkflowIDMaxLength(domainName),
		metrics.CadenceErrWorkflowIDExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeWorkflowID) {
		return validate.ErrWorkflowIDTooLong
	}
	if err := common.ValidateRetryPolicy(startRequest.RetryPolicy); err != nil {
		return err
	}
	wh.GetLogger().Debug(
		"Received StartWorkflowExecution. WorkflowID",
		tag.WorkflowID(startRequest.GetWorkflowID()))
	if !common.IsValidIDLength(
		startRequest.WorkflowType.GetName(),
		scope,
		idLengthWarnLimit,
		wh.config.WorkflowTypeMaxLength(domainName),
		metrics.CadenceErrWorkflowTypeExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeWorkflowType) {
		return validate.ErrWorkflowTypeTooLong
	}
	if err := wh.validateTaskList(startRequest.TaskList, scope, domainName); err != nil {
		return err
	}
	if startRequest.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		return validate.ErrInvalidExecutionStartToCloseTimeoutSeconds
	}
	if startRequest.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		return validate.ErrInvalidTaskStartToCloseTimeoutSeconds
	}
	if startRequest.GetDelayStartSeconds() < 0 {
		return validate.ErrInvalidDelayStartSeconds
	}
	if startRequest.GetJitterStartSeconds() < 0 {
		return validate.ErrInvalidJitterStartSeconds
	}
	jitter := startRequest.GetJitterStartSeconds()
	cron := startRequest.GetCronSchedule()
	if cron != "" {
		if _, err := backoff.ValidateSchedule(startRequest.GetCronSchedule()); err != nil {
			return err
		}
	}
	if jitter > 0 && cron != "" {
		// Calculate the cron duration and ensure that jitter is not greater than the cron duration,
		// because that would be confusing to users.

		// Request using start/end time zero value, which will get us an exact answer (i.e. its not in the
		// middle of a minute)
		backoffSeconds, err := backoff.GetBackoffForNextScheduleInSeconds(cron, time.Time{}, time.Time{}, jitter)
		if err != nil {
			return err
		}
		if jitter > backoffSeconds {
			return validate.ErrInvalidJitterStartSeconds2
		}
	}
	if !common.IsValidIDLength(
		startRequest.GetRequestID(),
		scope,
		idLengthWarnLimit,
		wh.config.RequestIDMaxLength(domainName),
		metrics.CadenceErrRequestIDExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeRequestID) {
		return validate.ErrRequestIDTooLong
	}
	if err := wh.searchAttributesValidator.ValidateSearchAttributes(startRequest.SearchAttributes, domainName); err != nil {
		return err
	}
	wh.GetLogger().Debug("Start workflow execution request domain", tag.WorkflowDomainName(domainName))
	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return err
	}
	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)
	actualSize := len(startRequest.Input)
	if startRequest.Memo != nil {
		actualSize += common.GetSizeOfMapStringToByteArray(startRequest.Memo.GetFields())
	}
	if err := common.CheckEventBlobSizeLimit(
		actualSize,
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		startRequest.GetWorkflowID(),
		"",
		scope,
		wh.GetThrottledLogger(),
		tag.BlobSizeViolationOperation("StartWorkflowExecution"),
	); err != nil {
		return err
	}
	isolationGroup := wh.getIsolationGroup(ctx, domainName)
	if !wh.isIsolationGroupHealthy(ctx, domainName, isolationGroup) {
		return &types.BadRequestError{fmt.Sprintf("Domain %s is drained from isolation group %s.", domainName, isolationGroup)}
	}
	return nil
}

// GetWorkflowExecutionHistory - retrieves the history of workflow execution
func (wh *WorkflowHandler) GetWorkflowExecutionHistory(
	ctx context.Context,
	getRequest *types.GetWorkflowExecutionHistoryRequest,
) (resp *types.GetWorkflowExecutionHistoryResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if getRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	domainName := getRequest.GetDomain()
	wfExecution := getRequest.GetExecution()

	if domainName == "" {
		return nil, validate.ErrDomainNotSet
	}

	if err := validate.CheckExecution(wfExecution); err != nil {
		return nil, err
	}

	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return nil, err
	}

	if getRequest.GetMaximumPageSize() <= 0 {
		getRequest.MaximumPageSize = int32(wh.config.HistoryMaxPageSize(getRequest.GetDomain()))
	}
	// force limit page size if exceed
	if getRequest.GetMaximumPageSize() > common.GetHistoryMaxPageSize {
		wh.GetThrottledLogger().Warn("GetHistory page size is larger than threshold",
			tag.WorkflowID(getRequest.Execution.GetWorkflowID()),
			tag.WorkflowRunID(getRequest.Execution.GetRunID()),
			tag.WorkflowDomainID(domainID),
			tag.WorkflowSize(int64(getRequest.GetMaximumPageSize())))
		getRequest.MaximumPageSize = common.GetHistoryMaxPageSize
	}

	scope := getMetricsScopeWithDomain(metrics.FrontendGetWorkflowExecutionHistoryScope, getRequest, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	if !getRequest.GetSkipArchival() {
		enableArchivalRead := wh.GetArchivalMetadata().GetHistoryConfig().ReadEnabled()
		historyArchived := wh.historyArchived(ctx, getRequest, domainID)
		if enableArchivalRead && historyArchived {
			return wh.getArchivedHistory(ctx, getRequest, domainID)
		}
	}

	// this function return the following 6 things,
	// 1. branch token
	// 2. the workflow run ID
	// 3. the last first event ID (the event ID of the last batch of events in the history)
	// 4. the next event ID
	// 5. whether the workflow is closed
	// 6. error if any
	queryHistory := func(
		domainUUID string,
		execution *types.WorkflowExecution,
		expectedNextEventID int64,
		currentBranchToken []byte,
	) ([]byte, string, int64, int64, bool, error) {
		response, err := wh.GetHistoryClient().PollMutableState(ctx, &types.PollMutableStateRequest{
			DomainUUID:          domainUUID,
			Execution:           execution,
			ExpectedNextEventID: expectedNextEventID,
			CurrentBranchToken:  currentBranchToken,
		})

		if err != nil {
			return nil, "", 0, 0, false, err
		}
		isWorkflowRunning := response.GetWorkflowCloseState() == persistence.WorkflowCloseStatusNone

		return response.CurrentBranchToken,
			response.Execution.GetRunID(),
			response.GetLastFirstEventID(),
			response.GetNextEventID(),
			isWorkflowRunning,
			nil
	}

	isLongPoll := getRequest.GetWaitForNewEvent()
	isCloseEventOnly := getRequest.GetHistoryEventFilterType() == types.HistoryEventFilterTypeCloseEvent
	execution := getRequest.Execution
	token := &getHistoryContinuationToken{}

	var runID string
	lastFirstEventID := common.FirstEventID
	var nextEventID int64
	var isWorkflowRunning bool

	// process the token for paging
	queryNextEventID := common.EndEventID
	if getRequest.NextPageToken != nil {
		token, err = deserializeHistoryToken(getRequest.NextPageToken)
		if err != nil {
			return nil, validate.ErrInvalidNextPageToken
		}
		if execution.RunID != "" && execution.GetRunID() != token.RunID {
			return nil, validate.ErrNextPageTokenRunIDMismatch
		}

		execution.RunID = token.RunID

		// we need to update the current next event ID and whether workflow is running
		if len(token.PersistenceToken) == 0 && isLongPoll && token.IsWorkflowRunning {
			logger := wh.GetLogger().WithTags(
				tag.WorkflowDomainName(getRequest.GetDomain()),
				tag.WorkflowID(getRequest.Execution.GetWorkflowID()),
				tag.WorkflowRunID(getRequest.Execution.GetRunID()),
			)
			// TODO: for now we only log the invalid timeout (this is done inside the helper function) in case
			// this change breaks existing customers. Once we are sure no one is calling this API with very short timeout
			// we can return the error.
			_ = common.ValidateLongPollContextTimeout(ctx, "GetWorkflowExecutionHistory", logger)

			if !isCloseEventOnly {
				queryNextEventID = token.NextEventID
			}
			token.BranchToken, _, lastFirstEventID, nextEventID, isWorkflowRunning, err =
				queryHistory(domainID, execution, queryNextEventID, token.BranchToken)
			if err != nil {
				return nil, err
			}
			token.FirstEventID = token.NextEventID
			token.NextEventID = nextEventID
			token.IsWorkflowRunning = isWorkflowRunning
		}
	} else {
		if !isCloseEventOnly {
			queryNextEventID = common.FirstEventID
		}
		token.BranchToken, runID, lastFirstEventID, nextEventID, isWorkflowRunning, err =
			queryHistory(domainID, execution, queryNextEventID, nil)
		if err != nil {
			return nil, err
		}

		execution.RunID = runID

		token.RunID = runID
		token.FirstEventID = common.FirstEventID
		token.NextEventID = nextEventID
		token.IsWorkflowRunning = isWorkflowRunning
		token.PersistenceToken = nil
	}

	call := yarpc.CallFromContext(ctx)
	clientFeatureVersion := call.Header(common.FeatureVersionHeaderName)
	clientImpl := call.Header(common.ClientImplHeaderName)
	supportsRawHistoryQuery := wh.versionChecker.SupportsRawHistoryQuery(clientImpl, clientFeatureVersion) == nil
	isRawHistoryEnabled := wh.config.SendRawWorkflowHistory(domainName) && supportsRawHistoryQuery

	history := &types.History{}
	history.Events = []*types.HistoryEvent{}
	var historyBlob []*types.DataBlob

	// helper function to just getHistory
	getHistory := func(firstEventID, nextEventID int64, nextPageToken []byte) error {
		if isRawHistoryEnabled {
			historyBlob, token.PersistenceToken, err = wh.getRawHistory(
				ctx,
				scope,
				domainID,
				domainName,
				*execution,
				firstEventID,
				nextEventID,
				getRequest.GetMaximumPageSize(),
				nextPageToken,
				token.TransientDecision,
				token.BranchToken,
			)
		} else {
			history, token.PersistenceToken, err = wh.getHistory(
				ctx,
				scope,
				domainID,
				domainName,
				*execution,
				firstEventID,
				nextEventID,
				getRequest.GetMaximumPageSize(),
				nextPageToken,
				token.TransientDecision,
				token.BranchToken,
			)
		}
		if err != nil {
			return err
		}
		return nil
	}

	if isCloseEventOnly {
		if !isWorkflowRunning {
			if err := getHistory(lastFirstEventID, nextEventID, nil); err != nil {
				return nil, err
			}
			if isRawHistoryEnabled {
				// since getHistory func will not return empty history, so the below is safe
				historyBlob = historyBlob[len(historyBlob)-1:]
			} else {
				// since getHistory func will not return empty history, so the below is safe
				history.Events = history.Events[len(history.Events)-1:]
			}
			token = nil
		} else if isLongPoll {
			// set the persistence token to be nil so next time we will query history for updates
			token.PersistenceToken = nil
		} else {
			token = nil
		}
	} else {
		// return all events
		if token.FirstEventID >= token.NextEventID {
			// currently there is no new event
			history.Events = []*types.HistoryEvent{}
			if !isWorkflowRunning {
				token = nil
			}
		} else {
			if err := getHistory(token.FirstEventID, token.NextEventID, token.PersistenceToken); err != nil {
				return nil, err
			}
			// here, for long pull on history events, we need to intercept the paging token from cassandra
			// and do something clever
			if len(token.PersistenceToken) == 0 && (!token.IsWorkflowRunning || !isLongPoll) {
				// meaning, there is no more history to be returned
				token = nil
			}
		}
	}

	nextToken, err := serializeHistoryToken(token)
	if err != nil {
		return nil, err
	}
	return &types.GetWorkflowExecutionHistoryResponse{
		History:       history,
		RawHistory:    historyBlob,
		NextPageToken: nextToken,
		Archived:      false,
	}, nil
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a decision task being created for the execution.
func (wh *WorkflowHandler) SignalWorkflowExecution(
	ctx context.Context,
	signalRequest *types.SignalWorkflowExecutionRequest,
) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return err
	}

	if signalRequest == nil {
		return validate.ErrRequestNotSet
	}

	domainName := signalRequest.GetDomain()
	wfExecution := signalRequest.GetWorkflowExecution()

	if domainName == "" {
		return validate.ErrDomainNotSet
	}
	if err := validate.CheckExecution(wfExecution); err != nil {
		return err
	}

	scope := getMetricsScopeWithDomain(metrics.FrontendSignalWorkflowExecutionScope, signalRequest, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	idLengthWarnLimit := wh.config.MaxIDLengthWarnLimit()
	if !common.IsValidIDLength(
		domainName,
		scope,
		idLengthWarnLimit,
		wh.config.DomainNameMaxLength(domainName),
		metrics.CadenceErrDomainNameExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeDomainName) {
		return validate.ErrDomainTooLong
	}

	if signalRequest.GetSignalName() == "" {
		return validate.ErrSignalNameNotSet
	}

	if !common.IsValidIDLength(
		signalRequest.GetSignalName(),
		scope,
		idLengthWarnLimit,
		wh.config.SignalNameMaxLength(domainName),
		metrics.CadenceErrSignalNameExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeSignalName) {
		return validate.ErrSignalNameTooLong
	}

	if !common.IsValidIDLength(
		signalRequest.GetRequestID(),
		scope,
		idLengthWarnLimit,
		wh.config.RequestIDMaxLength(domainName),
		metrics.CadenceErrRequestIDExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeRequestID) {
		return validate.ErrRequestIDTooLong
	}

	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)
	if err := common.CheckEventBlobSizeLimit(
		len(signalRequest.Input),
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		signalRequest.GetWorkflowExecution().GetWorkflowID(),
		signalRequest.GetWorkflowExecution().GetRunID(),
		scope,
		wh.GetThrottledLogger(),
		tag.BlobSizeViolationOperation("SignalWorkflowExecution"),
	); err != nil {
		return err
	}

	isolationGroup := wh.getIsolationGroup(ctx, domainName)
	if !wh.isIsolationGroupHealthy(ctx, domainName, isolationGroup) {
		return &types.BadRequestError{fmt.Sprintf("Domain %s is drained from isolation group %s.", domainName, isolationGroup)}
	}

	err = wh.GetHistoryClient().SignalWorkflowExecution(ctx, &types.HistorySignalWorkflowExecutionRequest{
		DomainUUID:    domainID,
		SignalRequest: signalRequest,
	})
	if err != nil {
		return wh.normalizeVersionedErrors(ctx, err)
	}

	return nil
}

func (wh *WorkflowHandler) SignalWithStartWorkflowExecutionAsync(
	ctx context.Context,
	signalWithStartRequest *types.SignalWithStartWorkflowExecutionAsyncRequest,
) (resp *types.SignalWithStartWorkflowExecutionAsyncResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}
	scope := getMetricsScopeWithDomain(metrics.FrontendSignalWithStartWorkflowExecutionAsyncScope, signalWithStartRequest, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	// validate request before pushing to queue
	err := wh.validateSignalWithStartWorkflowExecutionRequest(ctx, signalWithStartRequest.SignalWithStartWorkflowExecutionRequest, scope)
	if err != nil {
		return nil, err
	}
	producer, err := wh.producerManager.GetProducerByDomain(signalWithStartRequest.GetDomain())
	if err != nil {
		return nil, err
	}

	// Serialize the message to be sent to the queue.
	// Avoid JSON because json encoding of requests excludes PII fields such as input. JSON encoded request are logged by acccess controlled api layer for audit purposes.
	payload, err := wh.thriftrwEncoder.Encode(thrift.FromSignalWithStartWorkflowExecutionAsyncRequest(signalWithStartRequest))
	if err != nil {
		return nil, fmt.Errorf("failed to encode SignalWithStartWorkflowExecutionAsyncRequest: %v", err)
	}
	scope.RecordTimer(metrics.AsyncRequestPayloadSize, time.Duration(len(payload)))

	// propagate the headers from the context to the message
	header := &shared.Header{
		Fields: map[string][]byte{},
	}
	for k, v := range yarpc.CallFromContext(ctx).OriginalHeaders() {
		header.Fields[k] = []byte(v)
	}
	messageType := sqlblobs.AsyncRequestTypeSignalWithStartWorkflowExecutionAsyncRequest
	message := &sqlblobs.AsyncRequestMessage{
		PartitionKey: common.StringPtr(signalWithStartRequest.GetWorkflowID()),
		Type:         &messageType,
		Header:       header,
		Encoding:     common.StringPtr(string(common.EncodingTypeThriftRW)),
		Payload:      payload,
	}
	err = producer.Publish(ctx, message)
	if err != nil {
		return nil, err
	}
	return &types.SignalWithStartWorkflowExecutionAsyncResponse{}, nil
}

// SignalWithStartWorkflowExecution is used to ensure sending a signal event to a workflow execution.
// If workflow is running, this results in WorkflowExecutionSignaled event recorded in the history
// and a decision task being created for the execution.
// If workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// event recorded in history, and a decision task being created for the execution
func (wh *WorkflowHandler) SignalWithStartWorkflowExecution(
	ctx context.Context,
	signalWithStartRequest *types.SignalWithStartWorkflowExecutionRequest,
) (resp *types.StartWorkflowExecutionResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	scope := getMetricsScopeWithDomain(metrics.FrontendSignalWithStartWorkflowExecutionScope, signalWithStartRequest, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	err := wh.validateSignalWithStartWorkflowExecutionRequest(ctx, signalWithStartRequest, scope)
	if err != nil {
		return nil, err
	}

	domainName := signalWithStartRequest.GetDomain()
	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return nil, err
	}
	resp, err = wh.GetHistoryClient().SignalWithStartWorkflowExecution(ctx, &types.HistorySignalWithStartWorkflowExecutionRequest{
		DomainUUID:             domainID,
		SignalWithStartRequest: signalWithStartRequest,
		PartitionConfig:        wh.getPartitionConfig(ctx, domainName),
	})
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (wh *WorkflowHandler) validateSignalWithStartWorkflowExecutionRequest(ctx context.Context, signalWithStartRequest *types.SignalWithStartWorkflowExecutionRequest, scope metrics.Scope) error {
	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return err
	}

	if signalWithStartRequest == nil {
		return validate.ErrRequestNotSet
	}

	domainName := signalWithStartRequest.GetDomain()
	if domainName == "" {
		return validate.ErrDomainNotSet
	}
	if signalWithStartRequest.GetWorkflowID() == "" {
		return validate.ErrWorkflowIDNotSet
	}

	idLengthWarnLimit := wh.config.MaxIDLengthWarnLimit()
	if !common.IsValidIDLength(
		domainName,
		scope,
		idLengthWarnLimit,
		wh.config.DomainNameMaxLength(domainName),
		metrics.CadenceErrDomainNameExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeDomainName) {
		return validate.ErrDomainTooLong
	}

	if !common.IsValidIDLength(
		signalWithStartRequest.GetWorkflowID(),
		scope,
		idLengthWarnLimit,
		wh.config.WorkflowIDMaxLength(domainName),
		metrics.CadenceErrWorkflowIDExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeWorkflowID) {
		return validate.ErrWorkflowIDTooLong
	}

	if signalWithStartRequest.GetSignalName() == "" {
		return validate.ErrSignalNameNotSet
	}

	if !common.IsValidIDLength(
		signalWithStartRequest.GetSignalName(),
		scope,
		idLengthWarnLimit,
		wh.config.SignalNameMaxLength(domainName),
		metrics.CadenceErrSignalNameExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeSignalName) {
		return validate.ErrSignalNameTooLong
	}

	if signalWithStartRequest.WorkflowType == nil || signalWithStartRequest.WorkflowType.GetName() == "" {
		return validate.ErrWorkflowTypeNotSet
	}

	if !common.IsValidIDLength(
		signalWithStartRequest.WorkflowType.GetName(),
		scope,
		idLengthWarnLimit,
		wh.config.WorkflowTypeMaxLength(domainName),
		metrics.CadenceErrWorkflowTypeExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeWorkflowType) {
		return validate.ErrWorkflowTypeTooLong
	}

	if err := wh.validateTaskList(signalWithStartRequest.TaskList, scope, domainName); err != nil {
		return err
	}

	if !common.IsValidIDLength(
		signalWithStartRequest.GetRequestID(),
		scope,
		idLengthWarnLimit,
		wh.config.RequestIDMaxLength(domainName),
		metrics.CadenceErrRequestIDExceededWarnLimit,
		domainName,
		wh.GetLogger(),
		tag.IDTypeRequestID) {
		return validate.ErrRequestIDTooLong
	}

	if signalWithStartRequest.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		return validate.ErrInvalidExecutionStartToCloseTimeoutSeconds
	}

	if signalWithStartRequest.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		return validate.ErrInvalidTaskStartToCloseTimeoutSeconds
	}

	if err := common.ValidateRetryPolicy(signalWithStartRequest.RetryPolicy); err != nil {
		return err
	}

	if signalWithStartRequest.GetCronSchedule() != "" {
		if _, err := backoff.ValidateSchedule(signalWithStartRequest.GetCronSchedule()); err != nil {
			return err
		}
	}

	if err := wh.searchAttributesValidator.ValidateSearchAttributes(signalWithStartRequest.SearchAttributes, domainName); err != nil {
		return err
	}

	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)
	if err := common.CheckEventBlobSizeLimit(
		len(signalWithStartRequest.SignalInput),
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		signalWithStartRequest.GetWorkflowID(),
		"",
		scope,
		wh.GetThrottledLogger(),
		tag.BlobSizeViolationOperation("SignalWithStartWorkflowExecution"),
	); err != nil {
		return err
	}
	actualSize := len(signalWithStartRequest.Input) + common.GetSizeOfMapStringToByteArray(signalWithStartRequest.Memo.GetFields())
	if err := common.CheckEventBlobSizeLimit(
		actualSize,
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		signalWithStartRequest.GetWorkflowID(),
		"",
		scope,
		wh.GetThrottledLogger(),
		tag.BlobSizeViolationOperation("SignalWithStartWorkflowExecution"),
	); err != nil {
		return err
	}

	isolationGroup := wh.getIsolationGroup(ctx, domainName)
	if !wh.isIsolationGroupHealthy(ctx, domainName, isolationGroup) {
		return &types.BadRequestError{fmt.Sprintf("Domain %s is drained from isolation group %s.", domainName, isolationGroup)}
	}
	return nil
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (wh *WorkflowHandler) TerminateWorkflowExecution(
	ctx context.Context,
	terminateRequest *types.TerminateWorkflowExecutionRequest,
) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return err
	}

	if terminateRequest == nil {
		return validate.ErrRequestNotSet
	}

	domainName := terminateRequest.GetDomain()
	wfExecution := terminateRequest.GetWorkflowExecution()
	if terminateRequest.GetDomain() == "" {
		return validate.ErrDomainNotSet
	}
	if err := validate.CheckExecution(wfExecution); err != nil {
		return err
	}

	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return err
	}

	err = wh.GetHistoryClient().TerminateWorkflowExecution(ctx, &types.HistoryTerminateWorkflowExecutionRequest{
		DomainUUID:       domainID,
		TerminateRequest: terminateRequest,
	})
	if err != nil {
		return wh.normalizeVersionedErrors(ctx, err)
	}

	return nil
}

// ResetWorkflowExecution reset an existing workflow execution to the nextFirstEventID
// in the history and immediately terminating the current execution instance.
func (wh *WorkflowHandler) ResetWorkflowExecution(
	ctx context.Context,
	resetRequest *types.ResetWorkflowExecutionRequest,
) (resp *types.ResetWorkflowExecutionResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if resetRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	domainName := resetRequest.GetDomain()
	wfExecution := resetRequest.GetWorkflowExecution()
	if domainName == "" {
		return nil, validate.ErrDomainNotSet
	}
	if err := validate.CheckExecution(wfExecution); err != nil {
		return nil, err
	}

	domainID, err := wh.GetDomainCache().GetDomainID(resetRequest.GetDomain())
	if err != nil {
		return nil, err
	}

	resp, err = wh.GetHistoryClient().ResetWorkflowExecution(ctx, &types.HistoryResetWorkflowExecutionRequest{
		DomainUUID:   domainID,
		ResetRequest: resetRequest,
	})
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// RequestCancelWorkflowExecution - requests to cancel a workflow execution
func (wh *WorkflowHandler) RequestCancelWorkflowExecution(
	ctx context.Context,
	cancelRequest *types.RequestCancelWorkflowExecutionRequest,
) (retError error) {
	if wh.isShuttingDown() {
		return validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return err
	}

	if cancelRequest == nil {
		return validate.ErrRequestNotSet
	}

	domainName := cancelRequest.GetDomain()
	wfExecution := cancelRequest.GetWorkflowExecution()
	if domainName == "" {
		return validate.ErrDomainNotSet
	}
	if err := validate.CheckExecution(wfExecution); err != nil {
		return err
	}

	domainID, err := wh.GetDomainCache().GetDomainID(cancelRequest.GetDomain())
	if err != nil {
		return err
	}

	err = wh.GetHistoryClient().RequestCancelWorkflowExecution(ctx, &types.HistoryRequestCancelWorkflowExecutionRequest{
		DomainUUID:    domainID,
		CancelRequest: cancelRequest,
	})
	if err != nil {
		return wh.normalizeVersionedErrors(ctx, err)
	}

	return nil
}

// ListOpenWorkflowExecutions - retrieves info for open workflow executions in a domain
func (wh *WorkflowHandler) ListOpenWorkflowExecutions(
	ctx context.Context,
	listRequest *types.ListOpenWorkflowExecutionsRequest,
) (resp *types.ListOpenWorkflowExecutionsResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if listRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	if listRequest.GetDomain() == "" {
		return nil, validate.ErrDomainNotSet
	}

	if listRequest.StartTimeFilter == nil {
		return nil, &types.BadRequestError{Message: "StartTimeFilter is required"}
	}

	if listRequest.StartTimeFilter.EarliestTime == nil {
		return nil, &types.BadRequestError{Message: "EarliestTime in StartTimeFilter is required"}
	}

	if listRequest.StartTimeFilter.LatestTime == nil {
		return nil, &types.BadRequestError{Message: "LatestTime in StartTimeFilter is required"}
	}

	if listRequest.StartTimeFilter.GetEarliestTime() > listRequest.StartTimeFilter.GetLatestTime() {
		return nil, &types.BadRequestError{Message: "EarliestTime in StartTimeFilter should not be larger than LatestTime"}
	}

	if listRequest.ExecutionFilter != nil && listRequest.TypeFilter != nil {
		return nil, &types.BadRequestError{
			Message: "Only one of ExecutionFilter or TypeFilter is allowed"}
	}

	if listRequest.GetMaximumPageSize() <= 0 {
		listRequest.MaximumPageSize = int32(wh.config.VisibilityMaxPageSize(listRequest.GetDomain()))
	}

	if wh.isListRequestPageSizeTooLarge(listRequest.GetMaximumPageSize(), listRequest.GetDomain()) {
		return nil, &types.BadRequestError{
			Message: fmt.Sprintf("Pagesize is larger than allow %d", wh.config.ESIndexMaxResultWindow())}
	}

	domain := listRequest.GetDomain()
	domainID, err := wh.GetDomainCache().GetDomainID(domain)
	if err != nil {
		return nil, err
	}

	baseReq := persistence.ListWorkflowExecutionsRequest{
		DomainUUID:    domainID,
		Domain:        domain,
		PageSize:      int(listRequest.GetMaximumPageSize()),
		NextPageToken: listRequest.NextPageToken,
		EarliestTime:  listRequest.StartTimeFilter.GetEarliestTime(),
		LatestTime:    listRequest.StartTimeFilter.GetLatestTime(),
	}

	var persistenceResp *persistence.ListWorkflowExecutionsResponse
	if listRequest.ExecutionFilter != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = validate.ErrNoPermission
		} else {
			persistenceResp, err = wh.GetVisibilityManager().ListOpenWorkflowExecutionsByWorkflowID(
				ctx,
				&persistence.ListWorkflowExecutionsByWorkflowIDRequest{
					ListWorkflowExecutionsRequest: baseReq,
					WorkflowID:                    listRequest.ExecutionFilter.GetWorkflowID(),
				})
		}
		wh.GetLogger().Debug("List open workflow with filter",
			tag.WorkflowDomainName(listRequest.GetDomain()), tag.WorkflowListWorkflowFilterByID)
	} else if listRequest.TypeFilter != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = validate.ErrNoPermission
		} else {
			persistenceResp, err = wh.GetVisibilityManager().ListOpenWorkflowExecutionsByType(
				ctx,
				&persistence.ListWorkflowExecutionsByTypeRequest{
					ListWorkflowExecutionsRequest: baseReq,
					WorkflowTypeName:              listRequest.TypeFilter.GetName(),
				},
			)
		}
		wh.GetLogger().Debug("List open workflow with filter",
			tag.WorkflowDomainName(listRequest.GetDomain()), tag.WorkflowListWorkflowFilterByType)
	} else {
		persistenceResp, err = wh.GetVisibilityManager().ListOpenWorkflowExecutions(ctx, &baseReq)
	}

	if err != nil {
		return nil, err
	}

	resp = &types.ListOpenWorkflowExecutionsResponse{}
	resp.Executions = persistenceResp.Executions
	resp.NextPageToken = persistenceResp.NextPageToken
	return resp, nil
}

// ListArchivedWorkflowExecutions - retrieves archived info for closed workflow executions in a domain
func (wh *WorkflowHandler) ListArchivedWorkflowExecutions(
	ctx context.Context,
	listRequest *types.ListArchivedWorkflowExecutionsRequest,
) (resp *types.ListArchivedWorkflowExecutionsResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if listRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	if listRequest.GetDomain() == "" {
		return nil, validate.ErrDomainNotSet
	}

	if listRequest.GetPageSize() <= 0 {
		listRequest.PageSize = int32(wh.config.VisibilityMaxPageSize(listRequest.GetDomain()))
	}

	maxPageSize := wh.config.VisibilityArchivalQueryMaxPageSize()
	if int(listRequest.GetPageSize()) > maxPageSize {
		return nil, &types.BadRequestError{
			Message: fmt.Sprintf("Pagesize is larger than allowed %d", maxPageSize)}
	}

	if !wh.GetArchivalMetadata().GetVisibilityConfig().ClusterConfiguredForArchival() {
		return nil, &types.BadRequestError{Message: "Cluster is not configured for visibility archival"}
	}

	if !wh.GetArchivalMetadata().GetVisibilityConfig().ReadEnabled() {
		return nil, &types.BadRequestError{Message: "Cluster is not configured for reading archived visibility records"}
	}

	entry, err := wh.GetDomainCache().GetDomain(listRequest.GetDomain())
	if err != nil {
		return nil, err
	}

	if entry.GetConfig().VisibilityArchivalStatus != types.ArchivalStatusEnabled {
		return nil, &types.BadRequestError{Message: "Domain is not configured for visibility archival"}
	}

	URI, err := archiver.NewURI(entry.GetConfig().VisibilityArchivalURI)
	if err != nil {
		return nil, err
	}

	visibilityArchiver, err := wh.GetArchiverProvider().GetVisibilityArchiver(URI.Scheme(), service.Frontend)
	if err != nil {
		return nil, err
	}

	archiverRequest := &archiver.QueryVisibilityRequest{
		DomainID:      entry.GetInfo().ID,
		PageSize:      int(listRequest.GetPageSize()),
		NextPageToken: listRequest.NextPageToken,
		Query:         listRequest.GetQuery(),
	}

	archiverResponse, err := visibilityArchiver.Query(ctx, URI, archiverRequest)
	if err != nil {
		return nil, err
	}

	// special handling of ExecutionTime for cron or retry
	for _, execution := range archiverResponse.Executions {
		if execution.GetExecutionTime() == 0 {
			execution.ExecutionTime = common.Int64Ptr(execution.GetStartTime())
		}
	}

	return &types.ListArchivedWorkflowExecutionsResponse{
		Executions:    archiverResponse.Executions,
		NextPageToken: archiverResponse.NextPageToken,
	}, nil
}

// ListClosedWorkflowExecutions - retrieves info for closed workflow executions in a domain
func (wh *WorkflowHandler) ListClosedWorkflowExecutions(
	ctx context.Context,
	listRequest *types.ListClosedWorkflowExecutionsRequest,
) (resp *types.ListClosedWorkflowExecutionsResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if listRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	if listRequest.GetDomain() == "" {
		return nil, validate.ErrDomainNotSet
	}

	if listRequest.StartTimeFilter == nil {
		return nil, &types.BadRequestError{Message: "StartTimeFilter is required"}
	}

	if listRequest.StartTimeFilter.EarliestTime == nil {
		return nil, &types.BadRequestError{Message: "EarliestTime in StartTimeFilter is required"}
	}

	if listRequest.StartTimeFilter.LatestTime == nil {
		return nil, &types.BadRequestError{Message: "LatestTime in StartTimeFilter is required"}
	}

	if listRequest.StartTimeFilter.GetEarliestTime() > listRequest.StartTimeFilter.GetLatestTime() {
		return nil, &types.BadRequestError{Message: "EarliestTime in StartTimeFilter should not be larger than LatestTime"}
	}

	filterCount := 0
	if listRequest.TypeFilter != nil {
		filterCount++
	}
	if listRequest.StatusFilter != nil {
		filterCount++
	}

	if filterCount > 1 {
		return nil, &types.BadRequestError{
			Message: "Only one of ExecutionFilter, TypeFilter or StatusFilter is allowed"}
	} // If ExecutionFilter is provided with one of TypeFilter or StatusFilter, use ExecutionFilter and ignore other filter

	if listRequest.GetMaximumPageSize() <= 0 {
		listRequest.MaximumPageSize = int32(wh.config.VisibilityMaxPageSize(listRequest.GetDomain()))
	}

	if wh.isListRequestPageSizeTooLarge(listRequest.GetMaximumPageSize(), listRequest.GetDomain()) {
		return nil, &types.BadRequestError{
			Message: fmt.Sprintf("Pagesize is larger than allow %d", wh.config.ESIndexMaxResultWindow())}
	}

	domain := listRequest.GetDomain()
	domainID, err := wh.GetDomainCache().GetDomainID(domain)
	if err != nil {
		return nil, err
	}

	baseReq := persistence.ListWorkflowExecutionsRequest{
		DomainUUID:    domainID,
		Domain:        domain,
		PageSize:      int(listRequest.GetMaximumPageSize()),
		NextPageToken: listRequest.NextPageToken,
		EarliestTime:  listRequest.StartTimeFilter.GetEarliestTime(),
		LatestTime:    listRequest.StartTimeFilter.GetLatestTime(),
	}

	var persistenceResp *persistence.ListWorkflowExecutionsResponse
	if listRequest.ExecutionFilter != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = validate.ErrNoPermission
		} else {
			persistenceResp, err = wh.GetVisibilityManager().ListClosedWorkflowExecutionsByWorkflowID(
				ctx,
				&persistence.ListWorkflowExecutionsByWorkflowIDRequest{
					ListWorkflowExecutionsRequest: baseReq,
					WorkflowID:                    listRequest.ExecutionFilter.GetWorkflowID(),
				},
			)
		}
		wh.GetLogger().Debug("List closed workflow with filter",
			tag.WorkflowDomainName(listRequest.GetDomain()), tag.WorkflowListWorkflowFilterByID)
	} else if listRequest.TypeFilter != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = validate.ErrNoPermission
		} else {
			persistenceResp, err = wh.GetVisibilityManager().ListClosedWorkflowExecutionsByType(
				ctx,
				&persistence.ListWorkflowExecutionsByTypeRequest{
					ListWorkflowExecutionsRequest: baseReq,
					WorkflowTypeName:              listRequest.TypeFilter.GetName(),
				},
			)
		}
		wh.GetLogger().Debug("List closed workflow with filter",
			tag.WorkflowDomainName(listRequest.GetDomain()), tag.WorkflowListWorkflowFilterByType)
	} else if listRequest.StatusFilter != nil {
		if wh.config.DisableListVisibilityByFilter(domain) {
			err = validate.ErrNoPermission
		} else {
			persistenceResp, err = wh.GetVisibilityManager().ListClosedWorkflowExecutionsByStatus(
				ctx,
				&persistence.ListClosedWorkflowExecutionsByStatusRequest{
					ListWorkflowExecutionsRequest: baseReq,
					Status:                        listRequest.GetStatusFilter(),
				},
			)
		}
		wh.GetLogger().Debug("List closed workflow with filter",
			tag.WorkflowDomainName(listRequest.GetDomain()), tag.WorkflowListWorkflowFilterByStatus)
	} else {
		persistenceResp, err = wh.GetVisibilityManager().ListClosedWorkflowExecutions(ctx, &baseReq)
	}

	if err != nil {
		return nil, err
	}

	resp = &types.ListClosedWorkflowExecutionsResponse{}
	resp.Executions = persistenceResp.Executions
	resp.NextPageToken = persistenceResp.NextPageToken
	return resp, nil
}

// ListWorkflowExecutions - retrieves info for workflow executions in a domain
func (wh *WorkflowHandler) ListWorkflowExecutions(
	ctx context.Context,
	listRequest *types.ListWorkflowExecutionsRequest,
) (resp *types.ListWorkflowExecutionsResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if listRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	if listRequest.GetDomain() == "" {
		return nil, validate.ErrDomainNotSet
	}

	if listRequest.GetPageSize() <= 0 {
		listRequest.PageSize = int32(wh.config.VisibilityMaxPageSize(listRequest.GetDomain()))
	}

	if wh.isListRequestPageSizeTooLarge(listRequest.GetPageSize(), listRequest.GetDomain()) {
		return nil, &types.BadRequestError{
			Message: fmt.Sprintf("Pagesize is larger than allow %d", wh.config.ESIndexMaxResultWindow())}
	}

	validatedQuery, err := wh.visibilityQueryValidator.ValidateQuery(listRequest.GetQuery())
	if err != nil {
		return nil, err
	}

	domain := listRequest.GetDomain()
	domainID, err := wh.GetDomainCache().GetDomainID(domain)
	if err != nil {
		return nil, err
	}

	req := &persistence.ListWorkflowExecutionsByQueryRequest{
		DomainUUID:    domainID,
		Domain:        domain,
		PageSize:      int(listRequest.GetPageSize()),
		NextPageToken: listRequest.NextPageToken,
		Query:         validatedQuery,
	}
	persistenceResp, err := wh.GetVisibilityManager().ListWorkflowExecutions(ctx, req)
	if err != nil {
		return nil, err
	}

	resp = &types.ListWorkflowExecutionsResponse{}
	resp.Executions = persistenceResp.Executions
	resp.NextPageToken = persistenceResp.NextPageToken
	return resp, nil
}

// ListAllWorkflowExecutions - retrieves info for all workflow executions in a domain
func (wh *WorkflowHandler) ListAllWorkflowExecutions(
	ctx context.Context,
	listRequest *types.ListAllWorkflowExecutionsRequest,
) (resp *types.ListAllWorkflowExecutionsResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if listRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	if listRequest.GetDomain() == "" {
		return nil, validate.ErrDomainNotSet
	}

	if listRequest.StartTimeFilter == nil {
		return nil, &types.BadRequestError{Message: "StartTimeFilter is required"}
	}

	if listRequest.StartTimeFilter.EarliestTime == nil {
		return nil, &types.BadRequestError{Message: "EarliestTime in StartTimeFilter is required"}
	}

	if listRequest.StartTimeFilter.LatestTime == nil {
		return nil, &types.BadRequestError{Message: "LatestTime in StartTimeFilter is required"}
	}

	if listRequest.StartTimeFilter.GetEarliestTime() > listRequest.StartTimeFilter.GetLatestTime() {
		return nil, &types.BadRequestError{Message: "EarliestTime in StartTimeFilter should not be larger than LatestTime"}
	}

	if listRequest.GetMaximumPageSize() <= 0 {
		listRequest.MaximumPageSize = int32(wh.config.VisibilityMaxPageSize(listRequest.GetDomain()))
	}

	if wh.isListRequestPageSizeTooLarge(listRequest.GetMaximumPageSize(), listRequest.GetDomain()) {
		return nil, &types.BadRequestError{
			Message: fmt.Sprintf("Pagesize is larger than allow %d", wh.config.ESIndexMaxResultWindow())}
	}

	domain := listRequest.GetDomain()
	domainID, err := wh.GetDomainCache().GetDomainID(domain)
	if err != nil {
		return nil, err
	}

	baseReq := persistence.ListWorkflowExecutionsRequest{
		DomainUUID:    domainID,
		Domain:        domain,
		PageSize:      int(listRequest.GetMaximumPageSize()),
		NextPageToken: listRequest.NextPageToken,
		EarliestTime:  listRequest.StartTimeFilter.GetEarliestTime(),
		LatestTime:    listRequest.StartTimeFilter.GetLatestTime(),
	}

	var persistenceResp *persistence.ListWorkflowExecutionsResponse

	persistenceResp, err = wh.GetVisibilityManager().ListAllWorkflowExecutions(
		ctx,
		&persistence.ListAllWorkflowExecutionsRequest{
			ListWorkflowExecutionsRequest: baseReq,
			PartialMatch:                  listRequest.PartialMatch,
			WorkflowSearchValue:           listRequest.WorkflowSearchValue,
		},
	)
	if err != nil {
		return nil, err
	}

	wh.GetLogger().Debug("List all workflows",
		tag.WorkflowDomainName(listRequest.GetDomain()), tag.WorkflowListWorkflowFilterByType)

	resp = &types.ListAllWorkflowExecutionsResponse{}
	resp.Executions = persistenceResp.Executions
	resp.NextPageToken = persistenceResp.NextPageToken
	return resp, nil
}

// RestartWorkflowExecution - retrieves info for an existing workflow then restarts it
func (wh *WorkflowHandler) RestartWorkflowExecution(ctx context.Context, request *types.RestartWorkflowExecutionRequest) (resp *types.RestartWorkflowExecutionResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, validate.ErrRequestNotSet
	}

	domainName := request.GetDomain()
	wfExecution := request.GetWorkflowExecution()

	if request.GetDomain() == "" {
		return nil, validate.ErrDomainNotSet
	}

	if err := validate.CheckExecution(wfExecution); err != nil {
		return nil, err
	}

	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return nil, err
	}

	isolationGroup := wh.getIsolationGroup(ctx, domainName)
	if !wh.isIsolationGroupHealthy(ctx, domainName, isolationGroup) {
		return nil, &types.BadRequestError{fmt.Sprintf("Domain %s is drained from isolation group %s.", domainName, isolationGroup)}
	}

	history, err := wh.GetWorkflowExecutionHistory(ctx, &types.GetWorkflowExecutionHistoryRequest{
		Domain: domainName,
		Execution: &types.WorkflowExecution{
			WorkflowID: wfExecution.WorkflowID,
			RunID:      wfExecution.RunID,
		},
		SkipArchival: true,
	})
	if err != nil {
		return nil, validate.ErrHistoryNotFound
	}
	startRequest := constructRestartWorkflowRequest(history.History.Events[0].WorkflowExecutionStartedEventAttributes,
		domainName, request.Identity, wfExecution.WorkflowID)
	req, err := common.CreateHistoryStartWorkflowRequest(domainID, startRequest, time.Now(), wh.getPartitionConfig(ctx, domainName))
	if err != nil {
		return nil, err
	}
	startResp, err := wh.GetHistoryClient().StartWorkflowExecution(ctx, req)
	if err != nil {
		return nil, wh.normalizeVersionedErrors(ctx, err)
	}
	resp = &types.RestartWorkflowExecutionResponse{
		RunID: startResp.RunID,
	}

	return resp, nil
}

// ScanWorkflowExecutions - retrieves info for large amount of workflow executions in a domain without order
func (wh *WorkflowHandler) ScanWorkflowExecutions(
	ctx context.Context,
	listRequest *types.ListWorkflowExecutionsRequest,
) (resp *types.ListWorkflowExecutionsResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if listRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	if listRequest.GetDomain() == "" {
		return nil, validate.ErrDomainNotSet
	}

	if listRequest.GetPageSize() <= 0 {
		listRequest.PageSize = int32(wh.config.VisibilityMaxPageSize(listRequest.GetDomain()))
	}

	if wh.isListRequestPageSizeTooLarge(listRequest.GetPageSize(), listRequest.GetDomain()) {
		return nil, &types.BadRequestError{
			Message: fmt.Sprintf("Pagesize is larger than allow %d", wh.config.ESIndexMaxResultWindow())}
	}

	validatedQuery, err := wh.visibilityQueryValidator.ValidateQuery(listRequest.GetQuery())
	if err != nil {
		return nil, err
	}

	domain := listRequest.GetDomain()
	domainID, err := wh.GetDomainCache().GetDomainID(domain)
	if err != nil {
		return nil, err
	}

	req := &persistence.ListWorkflowExecutionsByQueryRequest{
		DomainUUID:    domainID,
		Domain:        domain,
		PageSize:      int(listRequest.GetPageSize()),
		NextPageToken: listRequest.NextPageToken,
		Query:         validatedQuery,
	}
	persistenceResp, err := wh.GetVisibilityManager().ScanWorkflowExecutions(ctx, req)
	if err != nil {
		return nil, err
	}

	resp = &types.ListWorkflowExecutionsResponse{}
	resp.Executions = persistenceResp.Executions
	resp.NextPageToken = persistenceResp.NextPageToken
	return resp, nil
}

// CountWorkflowExecutions - count number of workflow executions in a domain
func (wh *WorkflowHandler) CountWorkflowExecutions(
	ctx context.Context,
	countRequest *types.CountWorkflowExecutionsRequest,
) (resp *types.CountWorkflowExecutionsResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if countRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	if countRequest.GetDomain() == "" {
		return nil, validate.ErrDomainNotSet
	}

	validatedQuery, err := wh.visibilityQueryValidator.ValidateQuery(countRequest.GetQuery())
	if err != nil {
		return nil, err
	}

	domain := countRequest.GetDomain()
	domainID, err := wh.GetDomainCache().GetDomainID(domain)
	if err != nil {
		return nil, err
	}

	req := &persistence.CountWorkflowExecutionsRequest{
		DomainUUID: domainID,
		Domain:     domain,
		Query:      validatedQuery,
	}
	persistenceResp, err := wh.GetVisibilityManager().CountWorkflowExecutions(ctx, req)
	if err != nil {
		return nil, err
	}

	resp = &types.CountWorkflowExecutionsResponse{
		Count: persistenceResp.Count,
	}
	return resp, nil
}

// GetSearchAttributes return valid indexed keys
func (wh *WorkflowHandler) GetSearchAttributes(ctx context.Context) (resp *types.GetSearchAttributesResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	keys := wh.config.ValidSearchAttributes()
	resp = &types.GetSearchAttributesResponse{
		Keys: wh.convertIndexedKeyToThrift(keys),
	}
	return resp, nil
}

// ResetStickyTaskList reset the volatile information in mutable state of a given workflow.
func (wh *WorkflowHandler) ResetStickyTaskList(
	ctx context.Context,
	resetRequest *types.ResetStickyTaskListRequest,
) (resp *types.ResetStickyTaskListResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if resetRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	domainName := resetRequest.GetDomain()
	wfExecution := resetRequest.GetExecution()

	if domainName == "" {
		return nil, validate.ErrDomainNotSet
	}

	if err := validate.CheckExecution(wfExecution); err != nil {
		return nil, err
	}

	domainID, err := wh.GetDomainCache().GetDomainID(resetRequest.GetDomain())
	if err != nil {
		return nil, err
	}

	_, err = wh.GetHistoryClient().ResetStickyTaskList(ctx, &types.HistoryResetStickyTaskListRequest{
		DomainUUID: domainID,
		Execution:  resetRequest.Execution,
	})
	if err != nil {
		return nil, wh.normalizeVersionedErrors(ctx, err)
	}
	return &types.ResetStickyTaskListResponse{}, nil
}

// QueryWorkflow returns query result for a specified workflow execution
func (wh *WorkflowHandler) QueryWorkflow(
	ctx context.Context,
	queryRequest *types.QueryWorkflowRequest,
) (resp *types.QueryWorkflowResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if queryRequest == nil {
		return nil, validate.ErrRequestNotSet
	}

	domainName := queryRequest.GetDomain()
	wfExecution := queryRequest.GetExecution()

	if domainName == "" {
		return nil, validate.ErrDomainNotSet
	}

	if err := validate.CheckExecution(wfExecution); err != nil {
		return nil, err
	}

	if wh.config.DisallowQuery(domainName) {
		return nil, validate.ErrQueryDisallowedForDomain
	}

	if queryRequest.Query == nil {
		return nil, validate.ErrQueryNotSet
	}

	if queryRequest.Query.GetQueryType() == "" {
		return nil, validate.ErrQueryTypeNotSet
	}

	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return nil, err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(domainName)
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(domainName)

	scope := getMetricsScopeWithDomain(metrics.FrontendQueryWorkflowScope, queryRequest, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	if err := common.CheckEventBlobSizeLimit(
		len(queryRequest.GetQuery().GetQueryArgs()),
		sizeLimitWarn,
		sizeLimitError,
		domainID,
		queryRequest.GetExecution().GetWorkflowID(),
		queryRequest.GetExecution().GetRunID(),
		scope,
		wh.GetThrottledLogger(),
		tag.BlobSizeViolationOperation("QueryWorkflow")); err != nil {
		return nil, err
	}

	req := &types.HistoryQueryWorkflowRequest{
		DomainUUID: domainID,
		Request:    queryRequest,
	}
	hResponse, err := wh.GetHistoryClient().QueryWorkflow(ctx, req)
	if err != nil {
		return nil, err
	}
	return hResponse.GetResponse(), nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (wh *WorkflowHandler) DescribeWorkflowExecution(
	ctx context.Context,
	request *types.DescribeWorkflowExecutionRequest,
) (resp *types.DescribeWorkflowExecutionResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, validate.ErrRequestNotSet
	}

	domainName := request.GetDomain()
	wfExecution := request.GetExecution()
	if domainName == "" {
		return nil, validate.ErrDomainNotSet
	}

	if err := validate.CheckExecution(wfExecution); err != nil {
		return nil, err
	}

	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, err
	}

	response, err := wh.GetHistoryClient().DescribeWorkflowExecution(ctx, &types.HistoryDescribeWorkflowExecutionRequest{
		DomainUUID: domainID,
		Request:    request,
	})

	if err != nil {
		return nil, err
	}

	return response, nil
}

// DescribeTaskList returns information about the target tasklist, right now this API returns the
// pollers which polled this tasklist in last few minutes. If includeTaskListStatus field is true,
// it will also return status of tasklist's ackManager (readLevel, ackLevel, backlogCountHint and taskIDBlock).
func (wh *WorkflowHandler) DescribeTaskList(
	ctx context.Context,
	request *types.DescribeTaskListRequest,
) (resp *types.DescribeTaskListResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, validate.ErrRequestNotSet
	}

	if request.GetDomain() == "" {
		return nil, validate.ErrDomainNotSet
	}

	domainID, err := wh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, err
	}

	scope := getMetricsScopeWithDomain(metrics.FrontendDescribeTaskListScope, request, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	if err := wh.validateTaskList(request.TaskList, scope, request.GetDomain()); err != nil {
		return nil, err
	}

	if request.TaskListType == nil {
		return nil, validate.ErrTaskListTypeNotSet
	}

	response, err := wh.GetMatchingClient().DescribeTaskList(ctx, &types.MatchingDescribeTaskListRequest{
		DomainUUID:  domainID,
		DescRequest: request,
	})
	if err != nil {
		return nil, err
	}

	return response, nil
}

// ListTaskListPartitions returns all the partition and host for a taskList
func (wh *WorkflowHandler) ListTaskListPartitions(
	ctx context.Context,
	request *types.ListTaskListPartitionsRequest,
) (resp *types.ListTaskListPartitionsResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if request == nil {
		return nil, validate.ErrRequestNotSet
	}

	if request.GetDomain() == "" {
		return nil, validate.ErrDomainNotSet
	}

	scope := getMetricsScopeWithDomain(metrics.FrontendListTaskListPartitionsScope, request, wh.GetMetricsClient()).Tagged(metrics.GetContextTags(ctx)...)
	if err := wh.validateTaskList(request.TaskList, scope, request.GetDomain()); err != nil {
		return nil, err
	}

	resp, err := wh.GetMatchingClient().ListTaskListPartitions(ctx, &types.MatchingListTaskListPartitionsRequest{
		Domain:   request.Domain,
		TaskList: request.TaskList,
	})
	return resp, err
}

// GetTaskListsByDomain returns all the partition and host for a taskList
func (wh *WorkflowHandler) GetTaskListsByDomain(
	ctx context.Context,
	request *types.GetTaskListsByDomainRequest,
) (resp *types.GetTaskListsByDomainResponse, retError error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}

	if request == nil {
		return nil, validate.ErrRequestNotSet
	}

	if request.GetDomain() == "" {
		return nil, validate.ErrDomainNotSet
	}

	resp, err := wh.GetMatchingClient().GetTaskListsByDomain(ctx, &types.GetTaskListsByDomainRequest{
		Domain: request.Domain,
	})
	return resp, err
}

// RefreshWorkflowTasks re-generates the workflow tasks
func (wh *WorkflowHandler) RefreshWorkflowTasks(
	ctx context.Context,
	request *types.RefreshWorkflowTasksRequest,
) (err error) {
	if request == nil {
		return validate.ErrRequestNotSet
	}
	if err := validate.CheckExecution(request.Execution); err != nil {
		return err
	}
	domainEntry, err := wh.GetDomainCache().GetDomain(request.GetDomain())
	if err != nil {
		return err
	}

	err = wh.GetHistoryClient().RefreshWorkflowTasks(ctx, &types.HistoryRefreshWorkflowTasksRequest{
		DomainUIID: domainEntry.GetInfo().ID,
		Request:    request,
	})
	if err != nil {
		return err
	}
	return nil
}

func (wh *WorkflowHandler) getRawHistory(
	ctx context.Context,
	scope metrics.Scope,
	domainID string,
	domainName string,
	execution types.WorkflowExecution,
	firstEventID int64,
	nextEventID int64,
	pageSize int32,
	nextPageToken []byte,
	transientDecision *types.TransientDecisionInfo,
	branchToken []byte,
) ([]*types.DataBlob, []byte, error) {
	rawHistory := []*types.DataBlob{}
	shardID := common.WorkflowIDToHistoryShard(execution.WorkflowID, wh.config.NumHistoryShards)

	resp, err := wh.GetHistoryManager().ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      int(pageSize),
		NextPageToken: nextPageToken,
		ShardID:       common.IntPtr(shardID),
		DomainName:    domainName,
	})
	if err != nil {
		return nil, nil, err
	}

	var encoding *types.EncodingType
	for _, data := range resp.HistoryEventBlobs {
		switch data.Encoding {
		case common.EncodingTypeJSON:
			encoding = types.EncodingTypeJSON.Ptr()
		case common.EncodingTypeThriftRW:
			encoding = types.EncodingTypeThriftRW.Ptr()
		default:
			panic(fmt.Sprintf("Invalid encoding type for raw history, encoding type: %s", data.Encoding))
		}
		rawHistory = append(rawHistory, &types.DataBlob{
			EncodingType: encoding,
			Data:         data.Data,
		})
	}

	if len(resp.NextPageToken) == 0 && transientDecision != nil {
		if err := wh.validateTransientDecisionEvents(nextEventID, transientDecision); err != nil {
			scope.IncCounter(metrics.CadenceErrIncompleteHistoryCounter)
			wh.GetLogger().Error("getHistory error",
				tag.WorkflowDomainID(domainID),
				tag.WorkflowID(execution.GetWorkflowID()),
				tag.WorkflowRunID(execution.GetRunID()),
				tag.Error(err))
		}
		blob, err := wh.GetPayloadSerializer().SerializeBatchEvents(
			[]*types.HistoryEvent{transientDecision.ScheduledEvent, transientDecision.StartedEvent}, common.EncodingTypeThriftRW)
		if err != nil {
			return nil, nil, err
		}
		rawHistory = append(rawHistory, &types.DataBlob{
			EncodingType: types.EncodingTypeThriftRW.Ptr(),
			Data:         blob.Data,
		})
	}

	return rawHistory, resp.NextPageToken, nil
}

func (wh *WorkflowHandler) getHistory(
	ctx context.Context,
	scope metrics.Scope,
	domainID string,
	domainName string,
	execution types.WorkflowExecution,
	firstEventID, nextEventID int64,
	pageSize int32,
	nextPageToken []byte,
	transientDecision *types.TransientDecisionInfo,
	branchToken []byte,
) (*types.History, []byte, error) {

	var size int

	isFirstPage := len(nextPageToken) == 0
	shardID := common.WorkflowIDToHistoryShard(execution.WorkflowID, wh.config.NumHistoryShards)
	var err error
	historyEvents, size, nextPageToken, err := persistenceutils.ReadFullPageV2Events(ctx, wh.GetHistoryManager(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      int(pageSize),
		NextPageToken: nextPageToken,
		ShardID:       common.IntPtr(shardID),
		DomainName:    domainName,
	})

	if err != nil {
		return nil, nil, err
	}

	scope.RecordTimer(metrics.HistorySize, time.Duration(size))

	isLastPage := len(nextPageToken) == 0
	if err := verifyHistoryIsComplete(
		historyEvents,
		firstEventID,
		nextEventID-1,
		isFirstPage,
		isLastPage,
		int(pageSize)); err != nil {
		scope.IncCounter(metrics.CadenceErrIncompleteHistoryCounter)
		wh.GetLogger().Error("getHistory: incomplete history",
			tag.WorkflowDomainID(domainID),
			tag.WorkflowID(execution.GetWorkflowID()),
			tag.WorkflowRunID(execution.GetRunID()),
			tag.Error(err))
		return nil, nil, err
	}

	if len(nextPageToken) == 0 && transientDecision != nil {
		if err := wh.validateTransientDecisionEvents(nextEventID, transientDecision); err != nil {
			scope.IncCounter(metrics.CadenceErrIncompleteHistoryCounter)
			wh.GetLogger().Error("getHistory error",
				tag.WorkflowDomainID(domainID),
				tag.WorkflowID(execution.GetWorkflowID()),
				tag.WorkflowRunID(execution.GetRunID()),
				tag.Error(err))
		}
		// Append the transient decision events once we are done enumerating everything from the events table
		historyEvents = append(historyEvents, transientDecision.ScheduledEvent, transientDecision.StartedEvent)
	}

	executionHistory := &types.History{}
	executionHistory.Events = historyEvents
	return executionHistory, nextPageToken, nil
}

func (wh *WorkflowHandler) validateTransientDecisionEvents(
	expectedNextEventID int64,
	decision *types.TransientDecisionInfo,
) error {

	if decision.ScheduledEvent.ID == expectedNextEventID &&
		decision.StartedEvent.ID == expectedNextEventID+1 {
		return nil
	}

	return fmt.Errorf(
		"invalid transient decision: "+
			"expectedScheduledEventID=%v expectedStartedEventID=%v but have scheduledEventID=%v startedEventID=%v",
		expectedNextEventID,
		expectedNextEventID+1,
		decision.ScheduledEvent.ID,
		decision.StartedEvent.ID)
}

func (wh *WorkflowHandler) validateTaskList(t *types.TaskList, scope metrics.Scope, domain string) error {
	if t == nil || t.GetName() == "" {
		return validate.ErrTaskListNotSet
	}

	if !common.IsValidIDLength(
		t.GetName(),
		scope,
		wh.config.MaxIDLengthWarnLimit(),
		wh.config.TaskListNameMaxLength(domain),
		metrics.CadenceErrTaskListNameExceededWarnLimit,
		domain,
		wh.GetLogger(),
		tag.IDTypeTaskListName) {
		return validate.ErrTaskListTooLong
	}
	return nil
}

func (wh *WorkflowHandler) createPollForDecisionTaskResponse(
	ctx context.Context,
	scope metrics.Scope,
	domainID string,
	matchingResp *types.MatchingPollForDecisionTaskResponse,
	branchToken []byte,
) (*types.PollForDecisionTaskResponse, error) {

	if matchingResp.WorkflowExecution == nil {
		// this will happen if there is no decision task to be send to worker / caller
		return &types.PollForDecisionTaskResponse{}, nil
	}

	var history *types.History
	var continuation []byte
	var err error

	if matchingResp.GetStickyExecutionEnabled() && matchingResp.Query != nil {
		// meaning sticky query, we should not return any events to worker
		// since query task only check the current status
		history = &types.History{
			Events: []*types.HistoryEvent{},
		}
	} else {
		// here we have 3 cases:
		// 1. sticky && non query task
		// 2. non sticky &&  non query task
		// 3. non sticky && query task
		// for 1, partial history have to be send back
		// for 2 and 3, full history have to be send back

		var persistenceToken []byte

		firstEventID := common.FirstEventID
		nextEventID := matchingResp.GetNextEventID()
		if matchingResp.GetStickyExecutionEnabled() {
			firstEventID = matchingResp.GetPreviousStartedEventID() + 1
		}
		domainName, dErr := wh.GetDomainCache().GetDomainName(domainID)
		if dErr != nil {
			return nil, dErr
		}
		scope = scope.Tagged(metrics.DomainTag(domainName))
		history, persistenceToken, err = wh.getHistory(
			ctx,
			scope,
			domainID,
			domainName,
			*matchingResp.WorkflowExecution,
			firstEventID,
			nextEventID,
			int32(wh.config.HistoryMaxPageSize(domainName)),
			nil,
			matchingResp.DecisionInfo,
			branchToken,
		)
		if err != nil {
			return nil, err
		}

		if len(persistenceToken) != 0 {
			continuation, err = serializeHistoryToken(&getHistoryContinuationToken{
				RunID:             matchingResp.WorkflowExecution.GetRunID(),
				FirstEventID:      firstEventID,
				NextEventID:       nextEventID,
				PersistenceToken:  persistenceToken,
				TransientDecision: matchingResp.DecisionInfo,
				BranchToken:       branchToken,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	resp := &types.PollForDecisionTaskResponse{
		TaskToken:                 matchingResp.TaskToken,
		WorkflowExecution:         matchingResp.WorkflowExecution,
		WorkflowType:              matchingResp.WorkflowType,
		PreviousStartedEventID:    matchingResp.PreviousStartedEventID,
		StartedEventID:            matchingResp.StartedEventID, // this field is not set for query tasks as there's no decision task started event
		Query:                     matchingResp.Query,
		BacklogCountHint:          matchingResp.BacklogCountHint,
		Attempt:                   matchingResp.Attempt,
		History:                   history,
		NextPageToken:             continuation,
		WorkflowExecutionTaskList: matchingResp.WorkflowExecutionTaskList,
		ScheduledTimestamp:        matchingResp.ScheduledTimestamp,
		StartedTimestamp:          matchingResp.StartedTimestamp,
		Queries:                   matchingResp.Queries,
		NextEventID:               matchingResp.NextEventID,
		TotalHistoryBytes:         matchingResp.TotalHistoryBytes,
	}

	return resp, nil
}

func verifyHistoryIsComplete(
	events []*types.HistoryEvent,
	expectedFirstEventID int64,
	expectedLastEventID int64,
	isFirstPage bool,
	isLastPage bool,
	pageSize int,
) error {

	nEvents := len(events)
	if nEvents == 0 {
		if isLastPage {
			// we seem to be returning a non-nil pageToken on the lastPage which
			// in turn cases the client to call getHistory again - only to find
			// there are no more events to consume - bail out if this is the case here
			return nil
		}
		return fmt.Errorf("invalid history: contains zero events")
	}

	firstEventID := events[0].ID
	lastEventID := events[nEvents-1].ID

	if !isFirstPage { // atleast one page of history has been read previously
		if firstEventID <= expectedFirstEventID {
			// not first page and no events have been read in the previous pages - not possible
			return &types.InternalServiceError{
				Message: fmt.Sprintf(
					"invalid history: expected first eventID to be > %v but got %v", expectedFirstEventID, firstEventID),
			}
		}
		expectedFirstEventID = firstEventID
	}

	if !isLastPage {
		// estimate lastEventID based on pageSize. This is a lower bound
		// since the persistence layer counts "batch of events" as a single page
		expectedLastEventID = expectedFirstEventID + int64(pageSize) - 1
	}

	nExpectedEvents := expectedLastEventID - expectedFirstEventID + 1

	if firstEventID == expectedFirstEventID &&
		((isLastPage && lastEventID == expectedLastEventID && int64(nEvents) == nExpectedEvents) ||
			(!isLastPage && lastEventID >= expectedLastEventID && int64(nEvents) >= nExpectedEvents)) {
		return nil
	}

	return &types.InternalServiceError{
		Message: fmt.Sprintf(
			"incomplete history: "+
				"expected events [%v-%v] but got events [%v-%v] of length %v:"+
				"isFirstPage=%v,isLastPage=%v,pageSize=%v",
			expectedFirstEventID,
			expectedLastEventID,
			firstEventID,
			lastEventID,
			nEvents,
			isFirstPage,
			isLastPage,
			pageSize),
	}
}

func deserializeHistoryToken(bytes []byte) (*getHistoryContinuationToken, error) {
	token := &getHistoryContinuationToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func serializeHistoryToken(token *getHistoryContinuationToken) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	bytes, err := json.Marshal(token)
	return bytes, err
}

func isFailoverRequest(updateRequest *types.UpdateDomainRequest) bool {
	return updateRequest.ActiveClusterName != nil
}

func isGraceFailoverRequest(updateRequest *types.UpdateDomainRequest) bool {
	return updateRequest.FailoverTimeoutInSeconds != nil
}

func (wh *WorkflowHandler) checkOngoingFailover(
	ctx context.Context,
	domainName *string,
) error {

	enabledClusters := wh.GetClusterMetadata().GetEnabledClusterInfo()
	respChan := make(chan *types.DescribeDomainResponse, len(enabledClusters))

	g := &errgroup.Group{}
	for clusterName := range enabledClusters {
		frontendClient := wh.GetRemoteFrontendClient(clusterName)
		g.Go(func() (e error) {
			defer func() { log.CapturePanic(recover(), wh.GetLogger(), &e) }()

			resp, _ := frontendClient.DescribeDomain(ctx, &types.DescribeDomainRequest{Name: domainName})
			respChan <- resp
			return nil
		})
	}
	g.Wait()
	close(respChan)

	var failoverVersion *int64
	for resp := range respChan {
		if resp == nil {
			return &types.InternalServiceError{
				Message: "Failed to verify failover version from all clusters",
			}
		}
		if failoverVersion == nil {
			failoverVersion = &resp.FailoverVersion
		}
		if *failoverVersion != resp.GetFailoverVersion() {
			return &types.BadRequestError{
				Message: "Concurrent failover is not allow.",
			}
		}
	}
	return nil
}

func (wh *WorkflowHandler) historyArchived(ctx context.Context, request *types.GetWorkflowExecutionHistoryRequest, domainID string) bool {
	if request.GetExecution() == nil || request.GetExecution().GetRunID() == "" {
		return false
	}
	getMutableStateRequest := &types.GetMutableStateRequest{
		DomainUUID: domainID,
		Execution:  request.Execution,
	}
	_, err := wh.GetHistoryClient().GetMutableState(ctx, getMutableStateRequest)
	if err == nil {
		return false
	}
	switch err.(type) {
	case *types.EntityNotExistsError:
		// the only case in which history is assumed to be archived is if getting mutable state returns entity not found error
		return true
	}
	return false
}

func (wh *WorkflowHandler) getArchivedHistory(
	ctx context.Context,
	request *types.GetWorkflowExecutionHistoryRequest,
	domainID string,
) (*types.GetWorkflowExecutionHistoryResponse, error) {
	entry, err := wh.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		return nil, err
	}

	URIString := entry.GetConfig().HistoryArchivalURI
	if URIString == "" {
		// if URI is empty, it means the domain has never enabled for archival.
		// the error is not "workflow has passed retention period", because
		// we have no way to tell if the requested workflow exists or not.
		return nil, validate.ErrHistoryNotFound
	}

	URI, err := archiver.NewURI(URIString)
	if err != nil {
		return nil, err
	}

	historyArchiver, err := wh.GetArchiverProvider().GetHistoryArchiver(URI.Scheme(), service.Frontend)
	if err != nil {
		return nil, err
	}

	resp, err := historyArchiver.Get(ctx, URI, &archiver.GetHistoryRequest{
		DomainID:      domainID,
		WorkflowID:    request.GetExecution().GetWorkflowID(),
		RunID:         request.GetExecution().GetRunID(),
		NextPageToken: request.GetNextPageToken(),
		PageSize:      int(request.GetMaximumPageSize()),
	})
	if err != nil {
		return nil, err
	}

	history := &types.History{}
	for _, batch := range resp.HistoryBatches {
		history.Events = append(history.Events, batch.Events...)
	}
	return &types.GetWorkflowExecutionHistoryResponse{
		History:       history,
		NextPageToken: resp.NextPageToken,
		Archived:      true,
	}, nil
}

func (wh *WorkflowHandler) convertIndexedKeyToThrift(keys map[string]interface{}) map[string]types.IndexedValueType {
	converted := make(map[string]types.IndexedValueType)
	for k, v := range keys {
		converted[k] = common.ConvertIndexedValueTypeToInternalType(v, wh.GetLogger())
	}
	return converted
}

func (wh *WorkflowHandler) isListRequestPageSizeTooLarge(pageSize int32, domain string) bool {
	return common.IsAdvancedVisibilityReadingEnabled(wh.config.EnableReadVisibilityFromES(domain), wh.config.IsAdvancedVisConfigExist) &&
		pageSize > int32(wh.config.ESIndexMaxResultWindow())
}

// GetClusterInfo return information about cadence deployment
func (wh *WorkflowHandler) GetClusterInfo(
	ctx context.Context,
) (resp *types.ClusterInfo, err error) {
	return &types.ClusterInfo{
		SupportedClientVersions: &types.SupportedClientVersions{
			GoSdk:   client.SupportedGoSDKVersion,
			JavaSdk: client.SupportedJavaSDKVersion,
		},
	}, nil
}

func checkFailOverPermission(config *config.Config, domainName string) error {
	if config.Lockdown(domainName) {
		return validate.ErrDomainInLockdown
	}
	return nil
}

type domainWrapper struct {
	domain string
}

func (d domainWrapper) GetDomain() string {
	return d.domain
}

func (hs HealthStatus) String() string {
	switch hs {
	case HealthStatusOK:
		return "OK"
	case HealthStatusWarmingUp:
		return "WarmingUp"
	case HealthStatusShuttingDown:
		return "ShuttingDown"
	default:
		return "unknown"
	}
}

func getDomainWfIDRunIDTags(
	domainName string,
	wf *types.WorkflowExecution,
) []tag.Tag {
	tags := []tag.Tag{tag.WorkflowDomainName(domainName)}
	if wf == nil {
		return tags
	}
	return append(
		tags,
		tag.WorkflowID(wf.GetWorkflowID()),
		tag.WorkflowRunID(wf.GetRunID()),
	)
}

func checkRequiredDomainDataKVs(requiredDomainDataKeys map[string]interface{}, domainData map[string]string) error {
	// check requiredDomainDataKeys
	for k := range requiredDomainDataKeys {
		_, ok := domainData[k]
		if !ok {
			return fmt.Errorf("domain data error, missing required key %v . All required keys: %v", k, requiredDomainDataKeys)
		}
	}
	return nil
}

// Some error types are introduced later that some clients might not support
// To make them backward compatible, we continue returning the legacy error types
// for older clients
func (wh *WorkflowHandler) normalizeVersionedErrors(ctx context.Context, err error) error {
	switch err.(type) {
	case *types.WorkflowExecutionAlreadyCompletedError:
		call := yarpc.CallFromContext(ctx)
		clientFeatureVersion := call.Header(common.FeatureVersionHeaderName)
		clientImpl := call.Header(common.ClientImplHeaderName)
		featureFlags := client.GetFeatureFlagsFromHeader(call)

		vErr := wh.versionChecker.SupportsWorkflowAlreadyCompletedError(clientImpl, clientFeatureVersion, featureFlags)
		if vErr == nil {
			return err
		}
		return &types.EntityNotExistsError{Message: "Workflow execution already completed."}
	default:
		return err
	}
}
func constructRestartWorkflowRequest(w *types.WorkflowExecutionStartedEventAttributes, domain string, identity string, workflowID string) *types.StartWorkflowExecutionRequest {

	startRequest := &types.StartWorkflowExecutionRequest{
		RequestID:  uuid.New().String(),
		Domain:     domain,
		WorkflowID: workflowID,
		WorkflowType: &types.WorkflowType{
			Name: w.WorkflowType.Name,
		},
		TaskList: &types.TaskList{
			Name: w.TaskList.Name,
		},
		Input:                               w.Input,
		ExecutionStartToCloseTimeoutSeconds: w.ExecutionStartToCloseTimeoutSeconds,
		TaskStartToCloseTimeoutSeconds:      w.TaskStartToCloseTimeoutSeconds,
		Identity:                            identity,
		WorkflowIDReusePolicy:               types.WorkflowIDReusePolicyTerminateIfRunning.Ptr(),
	}
	startRequest.CronSchedule = w.CronSchedule
	startRequest.RetryPolicy = w.RetryPolicy
	startRequest.DelayStartSeconds = w.FirstDecisionTaskBackoffSeconds
	startRequest.Header = w.Header
	startRequest.Memo = w.Memo
	startRequest.SearchAttributes = w.SearchAttributes

	return startRequest
}

func getMetricsScopeWithDomain(
	scope int,
	d domainGetter,
	metricsClient metrics.Client,
) metrics.Scope {
	var metricsScope metrics.Scope
	if d != nil {
		metricsScope = metricsClient.Scope(scope).Tagged(metrics.DomainTag(d.GetDomain()))
	} else {
		metricsScope = metricsClient.Scope(scope).Tagged(metrics.DomainUnknownTag())
	}
	return metricsScope
}
