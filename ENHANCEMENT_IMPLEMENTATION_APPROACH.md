## Enhancement Implementation Approach

### Context
This repository contains an AWS Lambda that orchestrates Telegence device synchronization via SQS and SQL staging tables. The primary entrypoint is `AltaworxTelegenceAWSGetDevices.Function` which:
- Reads SQS message attributes to build a `TelegenceGetDevicesSyncState`
- Branches work in `TryProcessDeviceListAsync` between:
  - Initial BAN status preparation (`ProcessBanListAsync` → `SaveBillingAccountNumberStatusStaging`)
  - Main device list processing (`ProcessDeviceListAsync` with batching/pagination)
  - Follow-up reconciliation for devices present in AMOP but not in API (`ProcessDeviceNotExistsStagingAsync`)
- Persists batches to staging via `AwsFunctionBase.SqlBulkCopy`
- Coordinates downstream processing by sending SQS messages:
  - `SendMessageToGetDevicesQueueAsync`
  - `SendMessageToGetDeviceUsageQueueAsync`
  - `SendMessageToGetDeviceDetailQueueAsync`

Key configuration and tunables (environment variables) used in `Function` include: `TelegenceDevicesGetURL`, `TelegenceDestinationQueueGetDevicesURL`, `TelegenceDeviceDetailGetURL`, `TelegenceDeviceUsageQueueURL`, `TelegenceBanDetailGetURL`, `ProxyUrl`, `BatchSize`, `MaxCyclesToProcess`.

Database integration relies on stored procedures for preparing BANs, counting devices, staging writes, and marking progress.

---

### Goal and Non-Goals
- **Goal**: Implement enhancement <fill in enhancement name> to improve/extend the Telegence device synchronization flow while maintaining reliability and operational safety.
- **Non-Goals**: Large refactors of unrelated components; changing external contracts without coordination; altering cross-team owned stored procedures without review.

---

### High-Level Design
- **Trigger and Inputs**: SQS messages containing attributes such as `CurrentPage`, `HasMoreData`, `CurrentServiceProviderId`, `InitializeProcessing`, `IsProcessDeviceNotExistsStaging`, `IsLastProcessDeviceNotExistsStaging`, `GroupNumber`, `RetryNumber`.
- **Control Flow Changes**: Describe where the new logic slots into one of the three main paths in `TryProcessDeviceListAsync`:
  - Initial BAN prep path
  - Device list pagination path
  - Device-not-exists reconciliation path
- **Data Model/Storage**: Define any new columns, tables, or staging artifacts; list stored procedure changes or additions.
- **Configuration**: New environment variables and safe defaults; note whether values are required or optional.
- **Messaging**: Any new SQS attributes or queues; ensure backward compatibility with existing consumers.
- **Reliability**: Respect `CommonConstants.REMAINING_TIME_CUT_OFF` and `RetryNumber` semantics; ensure idempotent behavior in staging operations.

---

### Detailed Implementation Plan
1. Requirements and Acceptance Criteria
   - Document user-facing and system-facing acceptance criteria.
   - Define success metrics (e.g., throughput, reduced errors, correctness conditions).

2. API/Message Contract Updates (if any)
   - Add/modify SQS message attributes. Keep names consistent with existing style (e.g., PascalCase keys).
   - Update `FunctionHandler` to parse the new attributes with logging similar to existing fields.

3. Control Flow Integration
   - If enhancement affects initial BAN prep: update `ProcessBanListAsync` and post-processing in `TryProcessDeviceListAsync`.
   - If enhancement affects pagination: update `ProcessDeviceListAsync`, including `GetTelegenceDevicesFromAPI`, list accumulation, and `SaveDevicesToStagingTable`.
   - If enhancement affects reconciliation: update `ProcessDeviceNotExistsStagingAsync` and the grouping/queue loop in `SendProcessDeviceNotExistsStagingMessagesToQueueAsync`.

4. Persistence and Stored Procedures
   - Enumerate new/changed stored procedures with clear inputs/outputs and expected row counts/timeouts.
   - Update bulk copy schema in `SaveDevicesToStagingTable` if columns change; provide column mapping if order differs.

5. Configuration and Secrets
   - Introduce new env vars via Lambda configuration; supply defaults when sensible (see `DEFAULT_BATCH_SIZE` pattern).
   - Validate using the `AwsFunctionBase` helpers (e.g., `GetIntValueFromEnvironmentVariable`) where available.

6. Logging, Metrics, and Tracing
   - Use `AwsFunctionBase.LogInfo` consistently. Include: desc, key values, and phase markers.
   - Emit logs around message handling, SQL writes (`SQL_BULK_COPY_START`), and SQS responses.
   - Plan CloudWatch metrics/alarms for error rates, queue depth, and processing latency.

7. Backward Compatibility
   - Treat new message attributes as optional when reading (feature flag pattern) until all producers are updated.
   - Guard new flows behind config toggles when possible.

8. Testing Strategy
   - Unit tests: logic that transforms sync state, message attribute parsing, and any new pure functions.
   - Integration tests: stored procedures (against a test DB), bulk copy writes, idempotency checks.
   - End-to-end in non-prod: fire SQS messages through the Lambda with representative payloads.
   - Negative tests: time budget cutoff behavior, transient API failures, SQL transient errors (covered via `PolicyFactory`).

9. Deployment and Rollout
   - Order of operations:
     1) Database migrations (backward compatible)
     2) Lambda config updates (env vars/secrets)
     3) Lambda code deploy
     4) Producer changes (if message schema expands)
   - Use gradual rollout and monitor metrics/alarms; implement quick rollback plan.

10. Operations and Runbook Updates
   - Document operational toggles (env flags), on-call playbook for common failures, and dashboards to watch.

---

### Code Touchpoints (Anchors)
- `AltaworxTelegenceAWSGetDevices.Function`
  - `FunctionHandler` (SQS attribute parsing)
  - `TryProcessDeviceListAsync` (main routing)
  - `ProcessBanListAsync`, `SaveBillingAccountNumberStatusStaging`
  - `ProcessDeviceListAsync` (pagination; `GetTelegenceDevicesFromAPI`; staging save; queueing)
  - `ProcessDeviceNotExistsStagingAsync` (reconciliation loop; device detail calls)
  - `SendMessageToGetDevicesQueueAsync`, `SendMessageToGetDeviceUsageQueueAsync`, `SendMessageToGetDeviceDetailQueueAsync`
- `Altaworx.AWS.Core.AwsFunctionBase`
  - `LogInfo`, `SqlBulkCopy`, environment variable helpers, `ParameterizedLog`
- `Altaworx.AWS.Core.ServiceProviderCommon`
  - `GetNextServiceProviderId`, `GetFANFilter`

Use these anchors to locate insertion points and ensure consistent patterns for logging, retries, SQL calls, and queue messaging.

---

### Risk Assessment and Mitigations
- **Time budget exhaustion**: Respect remaining time checks and increment `RetryNumber`; short-circuit loops as done in existing code.
- **Duplicate processing**: Keep writes idempotent; rely on staging tables and "mark processed" stored procedures.
- **Partial migrations**: Ensure DB changes are additive first; gate new code paths by env flags until migration completes.
- **Increased queue volume**: Validate `delaySeconds` and grouping to smooth load; monitor queue age.

---

### Open Questions
- Are there producer-side changes to SQS attributes or scheduling?
- Do new fields require schema changes in staging or downstream consumers?
- What are the success metrics and SLOs for this enhancement?

---

### Implementation Checklist (to be updated during development)
- [ ] Requirements finalized and acceptance criteria approved
- [ ] Stored procedures designed and reviewed
- [ ] Env vars defined and provisioned
- [ ] Code changes implemented with logging and retries
- [ ] Tests (unit/integration/e2e) passing in CI
- [ ] Non-prod validation complete; metrics/alarms verified
- [ ] Production rollout plan executed and validated
