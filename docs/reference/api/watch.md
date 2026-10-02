# Watch

Primitives for polling an external system and parking between polls. See the [watch-and-respond recipe](../../patterns/long-running-loops.md#recipe-watch-and-respond) for how they compose, and [`Context.wait`](runtime.md) for the underlying timer suspend.

::: agentflow.prebuilt.watch
    options:
      members:
        - WatchResult
        - Watcher
        - CommandWatcher
        - watch_node
        - route_watch
