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

package clusterredirection

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/types"
	frontendcfg "github.com/uber/cadence/service/frontend/config"
)

const (
	// DCRedirectionPolicyDefault means no redirection
	DCRedirectionPolicyDefault = ""
	// DCRedirectionPolicyNoop means no redirection
	DCRedirectionPolicyNoop = "noop"
	// DCRedirectionPolicySelectedAPIsForwarding means forwarding the following non-worker APIs based domain
	// 1. StartWorkflowExecution
	// 2. SignalWithStartWorkflowExecution
	// 3. SignalWorkflowExecution
	// 4. RequestCancelWorkflowExecution
	// 5. TerminateWorkflowExecution
	// 6. QueryWorkflowStrongConsistency
	// 7. ResetWorkflow
	// please also reference selectedAPIsForwardingRedirectionPolicyAPIAllowlist and DCRedirectionPolicySelectedAPIsForwardingV2
	DCRedirectionPolicySelectedAPIsForwarding = "selected-apis-forwarding"
	// DCRedirectionPolicySelectedAPIsForwardingV2 forwards everything in DCRedirectionPolicySelectedAPIsForwarding,
	// as well as activity completions (sync and async) and heartbeats.
	// This is done because activity results are generally deemed "useful" and relatively costly to re-do (when it is
	// even possible to redo), but activity workers themselves may be datacenter-specific.
	//
	// This will likely replace DCRedirectionPolicySelectedAPIsForwarding soon.
	//
	// 1-7. from DCRedirectionPolicySelectedAPIsForwarding
	// 8. RecordActivityTaskHeartbeat
	// 9. RecordActivityTaskHeartbeatByID
	// 10. RespondActivityTaskCanceled
	// 11. RespondActivityTaskCanceledByID
	// 12. RespondActivityTaskCompleted
	// 13. RespondActivityTaskCompletedByID
	// 14. RespondActivityTaskFailed
	// 15. RespondActivityTaskFailedByID
	// please also reference selectedAPIsForwardingRedirectionPolicyAPIAllowlistV2
	DCRedirectionPolicySelectedAPIsForwardingV2 = "selected-apis-forwarding-v2"
	// DCRedirectionPolicyAllDomainAPIsForwarding means forwarding all the worker and non-worker APIs based domain,
	// and falling back to DCRedirectionPolicySelectedAPIsForwarding when the current active cluster is not the
	// cluster migration target.
	DCRedirectionPolicyAllDomainAPIsForwarding = "all-domain-apis-forwarding"
	// DCRedirectionPolicyAllDomainAPIsForwardingV2 means forwarding all the worker and non-worker APIs based domain,
	// and falling back to DCRedirectionPolicySelectedAPIsForwardingV2 when the current active cluster is not the
	// cluster migration target.
	DCRedirectionPolicyAllDomainAPIsForwardingV2 = "all-domain-apis-forwarding-v2"
)

type (
	// ClusterRedirectionPolicy is a DC redirection policy interface
	ClusterRedirectionPolicy interface {
		WithDomainIDRedirect(ctx context.Context, domainID string, apiName string, call func(string) error) error
		WithDomainNameRedirect(ctx context.Context, domainName string, apiName string, call func(string) error) error
	}

	// noopRedirectionPolicy is DC redirection policy which does nothing
	noopRedirectionPolicy struct {
		currentClusterName string
	}

	// selectedOrAllAPIsForwardingRedirectionPolicy is a DC redirection policy
	// which (based on domain) forwards selected APIs calls or all domain APIs to active cluster
	selectedOrAllAPIsForwardingRedirectionPolicy struct {
		currentClusterName string
		config             *frontendcfg.Config
		domainCache        cache.DomainCache
		allDomainAPIs      bool
		selectedAPIs       map[string]struct{}
		targetCluster      string
	}
)

// selectedAPIsForwardingRedirectionPolicyAPIAllowlist contains a list of non-worker APIs which can be redirected.
// This is paired with DCRedirectionPolicySelectedAPIsForwarding - keep both lists up to date.
var selectedAPIsForwardingRedirectionPolicyAPIAllowlist = map[string]struct{}{
	"StartWorkflowExecution":           {},
	"SignalWithStartWorkflowExecution": {},
	"SignalWorkflowExecution":          {},
	"RequestCancelWorkflowExecution":   {},
	"TerminateWorkflowExecution":       {},
	"QueryWorkflowStrongConsistency":   {},
	"ResetWorkflowExecution":           {},
}

// selectedAPIsForwardingRedirectionPolicyAPIAllowlistV2 contains a list of non-worker APIs which can be redirected.
// This is paired with DCRedirectionPolicySelectedAPIsForwardingV2 - keep both lists up to date.
var selectedAPIsForwardingRedirectionPolicyAPIAllowlistV2 = map[string]struct{}{
	// from selectedAPIsForwardingRedirectionPolicyAPIAllowlist
	"StartWorkflowExecution":           {},
	"SignalWithStartWorkflowExecution": {},
	"SignalWorkflowExecution":          {},
	"RequestCancelWorkflowExecution":   {},
	"TerminateWorkflowExecution":       {},
	"QueryWorkflowStrongConsistency":   {},
	"ResetWorkflowExecution":           {},
	// additional endpoints
	"RecordActivityTaskHeartbeat":      {},
	"RecordActivityTaskHeartbeatByID":  {},
	"RespondActivityTaskCanceled":      {},
	"RespondActivityTaskCanceledByID":  {},
	"RespondActivityTaskCompleted":     {},
	"RespondActivityTaskCompletedByID": {},
	"RespondActivityTaskFailed":        {},
	"RespondActivityTaskFailedByID":    {},
}

// RedirectionPolicyGenerator generate corresponding redirection policy
func RedirectionPolicyGenerator(clusterMetadata cluster.Metadata, config *frontendcfg.Config,
	domainCache cache.DomainCache, policy config.ClusterRedirectionPolicy) ClusterRedirectionPolicy {
	switch policy.Policy {
	case DCRedirectionPolicyDefault:
		// default policy, noop
		return newNoopRedirectionPolicy(clusterMetadata.GetCurrentClusterName())
	case DCRedirectionPolicyNoop:
		return newNoopRedirectionPolicy(clusterMetadata.GetCurrentClusterName())
	case DCRedirectionPolicySelectedAPIsForwarding:
		currentClusterName := clusterMetadata.GetCurrentClusterName()
		return newSelectedOrAllAPIsForwardingPolicy(currentClusterName, config, domainCache, false, selectedAPIsForwardingRedirectionPolicyAPIAllowlist, "")
	case DCRedirectionPolicySelectedAPIsForwardingV2:
		currentClusterName := clusterMetadata.GetCurrentClusterName()
		return newSelectedOrAllAPIsForwardingPolicy(currentClusterName, config, domainCache, false, selectedAPIsForwardingRedirectionPolicyAPIAllowlistV2, "")
	case DCRedirectionPolicyAllDomainAPIsForwarding:
		currentClusterName := clusterMetadata.GetCurrentClusterName()
		return newSelectedOrAllAPIsForwardingPolicy(currentClusterName, config, domainCache, true, selectedAPIsForwardingRedirectionPolicyAPIAllowlist, policy.AllDomainApisForwardingTargetCluster)
	case DCRedirectionPolicyAllDomainAPIsForwardingV2:
		currentClusterName := clusterMetadata.GetCurrentClusterName()
		return newSelectedOrAllAPIsForwardingPolicy(currentClusterName, config, domainCache, true, selectedAPIsForwardingRedirectionPolicyAPIAllowlistV2, policy.AllDomainApisForwardingTargetCluster)

	default:
		panic(fmt.Sprintf("Unknown DC redirection policy %v", policy.Policy))
	}
}

// newNoopRedirectionPolicy is DC redirection policy which does nothing
func newNoopRedirectionPolicy(currentClusterName string) *noopRedirectionPolicy {
	return &noopRedirectionPolicy{
		currentClusterName: currentClusterName,
	}
}

// WithDomainIDRedirect redirect the API call based on domain ID
func (policy *noopRedirectionPolicy) WithDomainIDRedirect(ctx context.Context, domainID string, apiName string, call func(string) error) error {
	return call(policy.currentClusterName)
}

// WithDomainNameRedirect redirect the API call based on domain name
func (policy *noopRedirectionPolicy) WithDomainNameRedirect(ctx context.Context, domainName string, apiName string, call func(string) error) error {
	return call(policy.currentClusterName)
}

// newSelectedOrAllAPIsForwardingPolicy creates a forwarding policy for selected APIs based on domain
func newSelectedOrAllAPIsForwardingPolicy(currentClusterName string, config *frontendcfg.Config, domainCache cache.DomainCache, allDoaminAPIs bool, selectedAPIs map[string]struct{}, targetCluster string) *selectedOrAllAPIsForwardingRedirectionPolicy {
	return &selectedOrAllAPIsForwardingRedirectionPolicy{
		currentClusterName: currentClusterName,
		config:             config,
		domainCache:        domainCache,
		allDomainAPIs:      allDoaminAPIs,
		selectedAPIs:       selectedAPIs,
		targetCluster:      targetCluster,
	}
}

// WithDomainIDRedirect redirect the API call based on domain ID
func (policy *selectedOrAllAPIsForwardingRedirectionPolicy) WithDomainIDRedirect(ctx context.Context, domainID string, apiName string, call func(string) error) error {
	domainEntry, err := policy.domainCache.GetDomainByID(domainID)
	if err != nil {
		return err
	}
	if domainEntry.IsDeprecatedOrDeleted() {
		return &types.DomainNotActiveError{
			Message:        "domain is deprecated.",
			DomainName:     domainEntry.GetInfo().Name,
			CurrentCluster: policy.currentClusterName,
			ActiveCluster:  policy.currentClusterName,
		}
	}
	return policy.withRedirect(ctx, domainEntry, apiName, call)
}

// WithDomainNameRedirect redirect the API call based on domain name
func (policy *selectedOrAllAPIsForwardingRedirectionPolicy) WithDomainNameRedirect(ctx context.Context, domainName string, apiName string, call func(string) error) error {
	domainEntry, err := policy.domainCache.GetDomain(domainName)
	if err != nil {
		return err
	}
	if domainEntry.IsDeprecatedOrDeleted() {
		return &types.DomainNotActiveError{
			Message:        "domain is deprecated or deleted",
			DomainName:     domainName,
			CurrentCluster: policy.currentClusterName,
			ActiveCluster:  policy.currentClusterName,
		}
	}
	return policy.withRedirect(ctx, domainEntry, apiName, call)
}

func (policy *selectedOrAllAPIsForwardingRedirectionPolicy) withRedirect(ctx context.Context, domainEntry *cache.DomainCacheEntry, apiName string, call func(string) error) error {
	targetDC, enableDomainNotActiveForwarding := policy.getTargetClusterAndIsDomainNotActiveAutoForwarding(ctx, domainEntry, apiName)

	err := call(targetDC)

	targetDC, ok := policy.isDomainNotActiveError(err)
	if !ok || !enableDomainNotActiveForwarding {
		return err
	}
	return call(targetDC)
}

func (policy *selectedOrAllAPIsForwardingRedirectionPolicy) isDomainNotActiveError(err error) (string, bool) {
	domainNotActiveErr, ok := err.(*types.DomainNotActiveError)
	if !ok {
		return "", false
	}
	return domainNotActiveErr.ActiveCluster, true
}

// return two values: the target cluster name, and whether or not forwarding to the active cluster
func (policy *selectedOrAllAPIsForwardingRedirectionPolicy) getTargetClusterAndIsDomainNotActiveAutoForwarding(ctx context.Context, domainEntry *cache.DomainCacheEntry, apiName string) (string, bool) {
	if !domainEntry.IsGlobalDomain() {
		// do not do dc redirection if domain is local domain,
		// for global domains with 1 dc, it's still useful to do auto-forwarding during cluster migration
		return policy.currentClusterName, false
	}

	if !policy.config.EnableDomainNotActiveAutoForwarding(domainEntry.GetInfo().Name) {
		// do not do dc redirection if auto-forwarding dynamicconfig is not enabled
		return policy.currentClusterName, false
	}

	currentActiveCluster := domainEntry.GetReplicationConfig().ActiveClusterName
	if policy.allDomainAPIs {
		if policy.targetCluster == "" {
			return currentActiveCluster, true
		}
		if policy.targetCluster == currentActiveCluster {
			return currentActiveCluster, true
		}
		// fallback to selected APIs if targetCluster is not empty and not the same as currentActiveCluster
	}

	_, ok := policy.selectedAPIs[apiName]
	if !ok {
		// do not do dc redirection if API is not whitelisted
		return policy.currentClusterName, false
	}

	return currentActiveCluster, true
}
