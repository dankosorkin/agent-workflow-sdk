# HTTP server

The optional FastAPI adapter over a run queue. See the [Control plane over HTTP](../../durability/control-plane.md#over-http) guide.

Requires the `server` extra: `pip install 'agent-workflow-sdk[server]'`.

::: agentflow.server
    options:
      members:
        - create_app

## Wire models

The request and response bodies that cross the HTTP boundary. These are Pydantic models, so they also drive the generated OpenAPI schema served at `/openapi.json` and the interactive docs at `/docs`.

::: agentflow.server.app
    options:
      members:
        - EnqueueRequest
        - ResumeRequest
        - RunResponse
        - CancelResponse
        - StatsResponse
