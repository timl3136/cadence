package host

import (
	"fmt"
	"time"

	"github.com/pborman/uuid"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/types"
)

func (s *IntegrationSuite) TestCronWorkflowWithOverlapPolicy() {
	id := "integration-wf-cron-overlap-test"
	wt := "integration-wf-cron-overlap-type"
	tl := "integration-wf-cron-overlap-tasklist"
	identity := "worker1"
	cronSchedule := "@every 5s"

	workflowType := &types.WorkflowType{}
	workflowType.Name = wt

	taskList := &types.TaskList{}
	taskList.Name = tl

	// Test both overlap policies
	testCases := []struct {
		name              string
		cronOverlapPolicy types.CronOverlapPolicy
		expectedBehavior  string
	}{
		{
			name:              "SkippedPolicy",
			cronOverlapPolicy: types.CronOverlapPolicySkipped,
			expectedBehavior:  "skip_overlapped_runs",
		},
		{
			name:              "BufferOnePolicy",
			cronOverlapPolicy: types.CronOverlapPolicyBufferOne,
			expectedBehavior:  "start_immediately_after_completion",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			workflowID := id + "-" + tc.name

			request := &types.StartWorkflowExecutionRequest{
				RequestID:                           uuid.New(),
				Domain:                              s.DomainName,
				WorkflowID:                          workflowID,
				WorkflowType:                        workflowType,
				TaskList:                            taskList,
				Input:                               nil,
				ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
				TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
				Identity:                            identity,
				CronSchedule:                        cronSchedule,
				CronOverlapPolicy:                   &tc.cronOverlapPolicy,
			}

			ctx, cancel := createContext()
			defer cancel()
			we, err := s.Engine.StartWorkflowExecution(ctx, request)
			s.Nil(err)

			// Logging for debug
			fmt.Printf("StartWorkflowExecution RunID: %s\n", we.RunID)

			var executions []*types.WorkflowExecution
			attemptCount := 0
			executionTimes := make([]time.Time, 0)

			// Decision task handler that simulates long-running workflow
			dtHandler := func(execution *types.WorkflowExecution, wt *types.WorkflowType,
				previousStartedEventID, startedEventID int64, history *types.History) ([]byte, []*types.Decision, error) {
				executions = append(executions, execution)
				attemptCount++
				executionTimes = append(executionTimes, time.Now())

				if attemptCount == 1 {
					return nil, []*types.Decision{
						{
							DecisionType: types.DecisionTypeCompleteWorkflowExecution.Ptr(),
							CompleteWorkflowExecutionDecisionAttributes: &types.CompleteWorkflowExecutionDecisionAttributes{
								Result: []byte(fmt.Sprintf("execution-%d-complete", attemptCount)),
							},
						},
					}, nil
				}

				return nil, []*types.Decision{
					{
						DecisionType: types.DecisionTypeCompleteWorkflowExecution.Ptr(),
						CompleteWorkflowExecutionDecisionAttributes: &types.CompleteWorkflowExecutionDecisionAttributes{
							Result: []byte(fmt.Sprintf("execution-%d-complete", attemptCount)),
						},
					},
				}, nil
			}

			poller := &TaskPoller{
				Engine:          s.Engine,
				Domain:          s.DomainName,
				TaskList:        taskList,
				Identity:        identity,
				DecisionHandler: dtHandler,
				Logger:          s.Logger,
				T:               s.T(),
			}

			for i := 0; i < 3; i++ {
				_, err = poller.PollAndProcessDecisionTask(false, false)
				s.True(err == nil, err)
				time.Sleep(500 * time.Millisecond)
			}

			s.True(attemptCount >= 2, "Expected at least 2 executions, got %d", attemptCount)

			if len(executionTimes) >= 2 {
				for i := 1; i < len(executionTimes); i++ {
					timeDiff := executionTimes[i].Sub(executionTimes[i-1])

					if tc.cronOverlapPolicy == types.CronOverlapPolicySkipped {
						s.True(timeDiff >= 1500*time.Millisecond,
							"Expected execution %d to be spaced by at least 1.5s from previous, got %v",
							i, timeDiff)
					} else {
						s.True(timeDiff >= 0, "Expected non-negative time difference, got %v", timeDiff)
					}
				}
			}

			if len(executions) > 0 {
				events := s.getHistory(s.DomainName, executions[0])
				s.True(len(events) > 0, "Expected workflow history to have events")
				lastEvent := events[len(events)-1]
				s.Equal(types.EventTypeWorkflowExecutionContinuedAsNew, lastEvent.GetEventType())
				attributes := lastEvent.WorkflowExecutionContinuedAsNewEventAttributes
				s.Equal(types.ContinueAsNewInitiatorCronSchedule, attributes.GetInitiator())
			}

			ctx, cancel = createContext()
			defer cancel()
			terminateErr := s.Engine.TerminateWorkflowExecution(ctx, &types.TerminateWorkflowExecutionRequest{
				Domain: s.DomainName,
				WorkflowExecution: &types.WorkflowExecution{
					WorkflowID: workflowID,
				},
			})
			s.NoError(terminateErr)

			ctx, cancel = createContext()
			defer cancel()
			dweResponse, err := s.Engine.DescribeWorkflowExecution(ctx, &types.DescribeWorkflowExecutionRequest{
				Domain: s.DomainName,
				Execution: &types.WorkflowExecution{
					WorkflowID: workflowID,
					RunID:      we.RunID,
				},
			})
			s.Nil(err)
			s.NotNil(dweResponse.WorkflowExecutionInfo)
		})
	}
}
