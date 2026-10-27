# Synchronization and runtime overview

## How it works

```mermaid
flowchart LR
    Client["Dashboard / API client"] --> Orchestrator
    Discovery["LAN discovery"] --> Election["Visible-candidate election"]
    Election --> Orchestrator
    Orchestrator <-->|"paired mTLS"| Node["Worker node"]
    Orchestrator --> Task["Task planning + scheduler"]
    Task --> Node
    Client --> Router["Transparent router"]
    Router --> Node
    Orchestrator --> Harness["Pi / DeepSeek harness"]
    Harness --> Tools["Shared typed tools"]
    Node <-->|"bounded, watermark-based sync"| Memory["Cluster memory + task history"]
    Orchestrator <-->|"bounded, watermark-based sync"| Memory
```

Election ranks candidates visible to each instance; it is not consensus, so a
network partition can produce a local winner on each side. Pairing establishes
certificate pins for remote cluster traffic. Read the
[architecture guide](./architecture.md) for component boundaries and
failure assumptions.

Dashboard state updates are built off the request path, coalesced during bursts,
and skipped when no clients are subscribed. Unchanged snapshots are not resent,
but newly connected dashboards still receive current state. Service shutdown
cancels background synchronization and upstream event streams before stores close.
