# Plan executor

A planner emits a dependency graph of tasks as data; the executor runs them by readiness. See the [plan-as-data recipe](../../patterns/long-running-loops.md#recipe-plan-as-data-the-planner-emits-a-task-dag) guide.

::: agentflow.prebuilt.plan_executor
    options:
      members:
        - Task
        - TaskResult
        - PlanState
        - Handler
        - add_plan_executor
        - plan_complete
        - plan_stalled
