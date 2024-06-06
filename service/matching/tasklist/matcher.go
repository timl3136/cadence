// Copyright (c) 2019 Uber Technologies, Inc.
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
	"fmt"
	"time"

	"golang.org/x/time/rate"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/matching/config"
)

// TaskMatcher matches a task producer with a task consumer
// Producers are usually rpc calls from history or taskReader
// that drains backlog from db. Consumers are the task list pollers
type TaskMatcher struct {
	log log.Logger
	// synchronous task channel to match producer/consumer for any isolation group
	// tasks having no isolation requirement are added to this channel
	// and pollers from all isolation groups read from this channel
	taskC chan *InternalTask
	// synchronos task channels to match producer/consumer for a certain isolation group
	// the key is the name of the isolation group
	isolatedTaskC map[string]chan *InternalTask
	// synchronous task channel to match query task - the reason to have
	// separate channel for this is because there are cases when consumers
	// are interested in queryTasks but not others. Example is when domain is
	// not active in a cluster
	queryTaskC chan *InternalTask
	// ratelimiter that limits the rate at which tasks can be dispatched to consumers
	limiter *quotas.RateLimiter

	fwdr          *Forwarder
	scope         metrics.Scope // domain metric scope
	numPartitions func() int    // number of task list partitions
}

// ErrTasklistThrottled implies a tasklist was throttled
var ErrTasklistThrottled = errors.New("tasklist limit exceeded")

// newTaskMatcher returns an task matcher instance. The returned instance can be
// used by task producers and consumers to find a match. Both sync matches and non-sync
// matches should use this implementation
func newTaskMatcher(config *config.TaskListConfig, fwdr *Forwarder, scope metrics.Scope, isolationGroups []string, log log.Logger) *TaskMatcher {
	dPtr := config.TaskDispatchRPS
	limiter := quotas.NewRateLimiter(&dPtr, config.TaskDispatchRPSTTL, config.MinTaskThrottlingBurstSize())
	isolatedTaskC := make(map[string]chan *InternalTask)
	for _, g := range isolationGroups {
		isolatedTaskC[g] = make(chan *InternalTask)
	}
	return &TaskMatcher{
		log:           log,
		limiter:       limiter,
		scope:         scope,
		fwdr:          fwdr,
		taskC:         make(chan *InternalTask),
		isolatedTaskC: isolatedTaskC,
		queryTaskC:    make(chan *InternalTask),
		numPartitions: config.NumReadPartitions,
	}
}

// Offer offers a task to a potential consumer (poller)
// If the task is successfully matched with a consumer, this
// method will return true and no error. If the task is matched
// but consumer returned error, then this method will return
// true and error message. This method should not be used for query
// task. This method should ONLY be used for sync match.
//
// When a local poller is not available and forwarding to a parent
// task list partition is possible, this method will attempt forwarding
// to the parent partition.
//
// Cases when this method will block:
//
// Ratelimit:
// When a ratelimit token is not available, this method might block
// waiting for a token until the provided context timeout. Rate limits are
// not enforced for forwarded tasks from child partition.
//
// Forwarded tasks that originated from db backlog:
// When this method is called with a task that is forwarded from a
// remote partition and if (1) this task list is root (2) task
// was from db backlog - this method will block until context timeout
// trying to match with a poller. The caller is expected to set the
// correct context timeout.
//
// returns error when:
//   - ratelimit is exceeded (does not apply to query task)
//   - context deadline is exceeded
//   - task is matched and consumer returns error in response channel
func (tm *TaskMatcher) Offer(ctx context.Context, task *InternalTask) (bool, error) {
	var err error
	var rsv *rate.Reservation
	if !task.IsForwarded() {
		rsv, err = tm.ratelimit(ctx)
		if err != nil {
			tm.scope.IncCounter(metrics.SyncThrottlePerTaskListCounter)
			return false, err
		}
	}

	select {
	case tm.getTaskC(task) <- task: // poller picked up the task
		if task.ResponseC != nil {
			// if there is a response channel, block until resp is received
			// and return error if the response contains error
			err = <-task.ResponseC
			return true, err
		}
		return false, nil
	default:
		// no poller waiting for tasks, try forwarding this task to the
		// root partition if possible
		select {
		case token := <-tm.fwdrAddReqTokenC():
			if err := tm.fwdr.ForwardTask(ctx, task); err == nil {
				// task was remotely sync matched on the parent partition
				token.release("")
				return true, nil
			}
			token.release("")
		default:
			if !tm.isForwardingAllowed() && // we are the root partition and forwarding is not possible
				task.source == types.TaskSourceDbBacklog && // task was from backlog (stored in db)
				task.IsForwarded() { // task came from a child partition
				// a forwarded backlog task from a child partition, block trying
				// to match with a poller until ctx timeout
				return tm.offerOrTimeout(ctx, task)
			}
		}

		if rsv != nil {
			// there was a ratelimit token we consumed
			// return it since we did not really do any work
			rsv.Cancel()
		}
		return false, nil
	}
}

func (tm *TaskMatcher) offerOrTimeout(ctx context.Context, task *InternalTask) (bool, error) {
	select {
	case tm.getTaskC(task) <- task: // poller picked up the task
		if task.ResponseC != nil {
			select {
			case err := <-task.ResponseC:
				return true, err
			case <-ctx.Done():
				return false, nil
			}
		}
		return task.ActivityTaskDispatchInfo != nil, nil
	case <-ctx.Done():
		return false, nil
	}
}

// OfferQuery will either match task to local poller or will forward query task.
// Local match is always attempted before forwarding is attempted. If local match occurs
// response and error are both nil, if forwarding occurs then response or error is returned.
func (tm *TaskMatcher) OfferQuery(ctx context.Context, task *InternalTask) (*types.QueryWorkflowResponse, error) {
	select {
	case tm.queryTaskC <- task:
		<-task.ResponseC
		return nil, nil
	default:
	}

	fwdrTokenC := tm.fwdrAddReqTokenC()

	for {
		select {
		case tm.queryTaskC <- task:
			<-task.ResponseC
			return nil, nil
		case token := <-fwdrTokenC:
			resp, err := tm.fwdr.ForwardQueryTask(ctx, task)
			token.release("")
			if err == nil {
				return resp, nil
			}
			if err == errForwarderSlowDown {
				// if we are rate limited, try only local match for the
				// remainder of the context timeout left
				fwdrTokenC = noopForwarderTokenC
				continue
			}
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// MustOffer blocks until a consumer is found to handle this task
// Returns error only when context is canceled, expired or the ratelimit is set to zero (allow nothing)
func (tm *TaskMatcher) MustOffer(ctx context.Context, task *InternalTask) error {
	if _, err := tm.ratelimit(ctx); err != nil {
		return fmt.Errorf("rate limit error dispatching: %w", err)
	}

	// attempt a match with local poller first. When that
	// doesn't succeed, try both local match and remote match
	taskC := tm.getTaskC(task)
	select {
	case taskC <- task: // poller picked up the task
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context done when trying to forward local task: %w", ctx.Err())
	default:
	}

forLoop:
	for {
		select {
		case taskC <- task: // poller picked up the task
			return nil
		case token := <-tm.fwdrAddReqTokenC():
			childCtx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second*2))
			err := tm.fwdr.ForwardTask(childCtx, task)
			token.release("")
			if err != nil {

				tm.log.Debug("failed to forward task",
					tag.Error(err),
					tag.TaskID(task.Event.TaskID),
				)
				// forwarder returns error only when the call is rate limited. To
				// avoid a busy loop on such rate limiting events, we only attempt to make
				// the next forwarded call after this childCtx expires. Till then, we block
				// hoping for a local poller match
				select {
				case taskC <- task: // poller picked up the task
					cancel()
					return nil
				case <-childCtx.Done():
				case <-ctx.Done():
					cancel()
					return fmt.Errorf("failed to dispatch after failing to forward task: %w", ctx.Err())
				}
				cancel()
				continue forLoop
			}
			cancel()
			// at this point, we forwarded the task to a parent partition which
			// in turn dispatched the task to a poller. Make sure we delete the
			// task from the database
			task.Finish(nil)
			return nil
		case <-ctx.Done():
			return fmt.Errorf("failed to offer task: %w", ctx.Err())
		}
	}
}

// Poll blocks until a task is found or context deadline is exceeded
// On success, the returned task could be a query task or a regular task
// Returns ErrNoTasks when context deadline is exceeded
func (tm *TaskMatcher) Poll(ctx context.Context, isolationGroup string) (*InternalTask, error) {
	isolatedTaskC, ok := tm.isolatedTaskC[isolationGroup]
	if !ok && isolationGroup != "" {
		// fallback to default isolation group instead of making poller crash if the isolation group is invalid
		isolatedTaskC = tm.taskC
		tm.scope.IncCounter(metrics.PollerInvalidIsolationGroupCounter)
	}
	// try local match first without blocking until context timeout
	if task, err := tm.pollNonBlocking(ctx, isolatedTaskC, tm.taskC, tm.queryTaskC); err == nil {
		return task, nil
	}
	// there is no local poller available to pickup this task. Now block waiting
	// either for a local poller or a forwarding token to be available. When a
	// forwarding token becomes available, send this poll to a parent partition
	tm.log.Debug("falling back to non-local polling",
		tag.IsolationGroup(isolationGroup),
		tag.Dynamic("isolated channel", len(isolatedTaskC)),
		tag.Dynamic("fallback channel", len(tm.taskC)),
	)
	return tm.pollOrForward(ctx, isolationGroup, isolatedTaskC, tm.taskC, tm.queryTaskC)
}

// PollForQuery blocks until a *query* task is found or context deadline is exceeded
// Returns ErrNoTasks when context deadline is exceeded
func (tm *TaskMatcher) PollForQuery(ctx context.Context) (*InternalTask, error) {
	// try local match first without blocking until context timeout
	if task, err := tm.pollNonBlocking(ctx, nil, nil, tm.queryTaskC); err == nil {
		return task, nil
	}
	// there is no local poller available to pickup this task. Now block waiting
	// either for a local poller or a forwarding token to be available. When a
	// forwarding token becomes available, send this poll to a parent partition
	return tm.pollOrForward(ctx, "", nil, nil, tm.queryTaskC)
}

// UpdateRatelimit updates the task dispatch rate
func (tm *TaskMatcher) UpdateRatelimit(rps *float64) {
	if rps == nil {
		return
	}
	rate := *rps
	nPartitions := tm.numPartitions()
	if rate > float64(nPartitions) {
		// divide the rate equally across all partitions
		rate = rate / float64(tm.numPartitions())
	}
	tm.limiter.UpdateMaxDispatch(&rate)
}

// Rate returns the current rate at which tasks are dispatched
func (tm *TaskMatcher) Rate() float64 {
	return tm.limiter.Limit()
}

func (tm *TaskMatcher) pollOrForward(
	ctx context.Context,
	isolationGroup string,
	isolatedTaskC <-chan *InternalTask,
	taskC <-chan *InternalTask,
	queryTaskC <-chan *InternalTask,
) (*InternalTask, error) {
	select {
	case task := <-isolatedTaskC:
		if task.ResponseC != nil {
			tm.scope.IncCounter(metrics.PollSuccessWithSyncPerTaskListCounter)
		}
		tm.scope.IncCounter(metrics.PollSuccessPerTaskListCounter)
		return task, nil
	case task := <-taskC:
		if task.ResponseC != nil {
			tm.scope.IncCounter(metrics.PollSuccessWithSyncPerTaskListCounter)
		}
		tm.scope.IncCounter(metrics.PollSuccessPerTaskListCounter)
		return task, nil
	case task := <-queryTaskC:
		tm.scope.IncCounter(metrics.PollSuccessWithSyncPerTaskListCounter)
		tm.scope.IncCounter(metrics.PollSuccessPerTaskListCounter)
		return task, nil
	case <-ctx.Done():
		tm.scope.IncCounter(metrics.PollTimeoutPerTaskListCounter)
		return nil, ErrNoTasks
	case token := <-tm.fwdrPollReqTokenC(isolationGroup):
		if task, err := tm.fwdr.ForwardPoll(ctx); err == nil {
			token.release(isolationGroup)
			return task, nil
		}
		token.release(isolationGroup)
		return tm.poll(ctx, isolatedTaskC, taskC, queryTaskC)
	}
}

func (tm *TaskMatcher) poll(
	ctx context.Context,
	isolatedTaskC <-chan *InternalTask,
	taskC <-chan *InternalTask,
	queryTaskC <-chan *InternalTask,
) (*InternalTask, error) {
	select {
	case task := <-isolatedTaskC:
		if task.ResponseC != nil {
			tm.scope.IncCounter(metrics.PollSuccessWithSyncPerTaskListCounter)
		}
		tm.scope.IncCounter(metrics.PollSuccessPerTaskListCounter)
		return task, nil
	case task := <-taskC:
		if task.ResponseC != nil {
			tm.scope.IncCounter(metrics.PollSuccessWithSyncPerTaskListCounter)
		}
		tm.scope.IncCounter(metrics.PollSuccessPerTaskListCounter)
		return task, nil
	case task := <-queryTaskC:
		tm.scope.IncCounter(metrics.PollSuccessWithSyncPerTaskListCounter)
		tm.scope.IncCounter(metrics.PollSuccessPerTaskListCounter)
		return task, nil
	case <-ctx.Done():
		tm.scope.IncCounter(metrics.PollTimeoutPerTaskListCounter)
		return nil, ErrNoTasks
	}
}

func (tm *TaskMatcher) pollNonBlocking(
	ctx context.Context,
	isolatedTaskC <-chan *InternalTask,
	taskC <-chan *InternalTask,
	queryTaskC <-chan *InternalTask,
) (*InternalTask, error) {
	select {
	case task := <-isolatedTaskC:
		if task.ResponseC != nil {
			tm.scope.IncCounter(metrics.PollSuccessWithSyncPerTaskListCounter)
		}
		tm.scope.IncCounter(metrics.PollSuccessPerTaskListCounter)
		return task, nil
	case task := <-taskC:
		if task.ResponseC != nil {
			tm.scope.IncCounter(metrics.PollSuccessWithSyncPerTaskListCounter)
		}
		tm.scope.IncCounter(metrics.PollSuccessPerTaskListCounter)
		return task, nil
	case task := <-queryTaskC:
		tm.scope.IncCounter(metrics.PollSuccessWithSyncPerTaskListCounter)
		tm.scope.IncCounter(metrics.PollSuccessPerTaskListCounter)
		return task, nil
	default:
		return nil, ErrNoTasks
	}
}

func (tm *TaskMatcher) fwdrPollReqTokenC(isolationGroup string) <-chan *ForwarderReqToken {
	if tm.fwdr == nil {
		return noopForwarderTokenC
	}
	return tm.fwdr.PollReqTokenC(isolationGroup)
}

func (tm *TaskMatcher) fwdrAddReqTokenC() <-chan *ForwarderReqToken {
	if tm.fwdr == nil {
		return noopForwarderTokenC
	}
	return tm.fwdr.AddReqTokenC()
}

func (tm *TaskMatcher) ratelimit(ctx context.Context) (*rate.Reservation, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		if err := tm.limiter.Wait(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}

	rsv := tm.limiter.Reserve()
	// If we have to wait too long for reservation, give up and return
	if !rsv.OK() || rsv.Delay() > time.Until(deadline) {
		if rsv.OK() { // if we were indeed given a reservation, return it before we bail out
			rsv.Cancel()
		}
		return nil, ErrTasklistThrottled
	}

	time.Sleep(rsv.Delay())
	return rsv, nil
}

func (tm *TaskMatcher) isForwardingAllowed() bool {
	return tm.fwdr != nil
}

func (tm *TaskMatcher) getTaskC(task *InternalTask) chan<- *InternalTask {
	taskC := tm.taskC
	if isolatedTaskC, ok := tm.isolatedTaskC[task.isolationGroup]; ok && task.isolationGroup != "" {
		taskC = isolatedTaskC
	}
	return taskC
}
