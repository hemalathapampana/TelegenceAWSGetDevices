## Telegence sync: notifications and billing periods

### Audience
- Engineering, Ops

### TL;DR
- **Start/end notifications**: Not sent. The Lambda writes structured logs only.
- **Error notifications**: Not sent. Errors are caught and logged; no SNS/SES/Email/Slack dispatch.
- **Billing periods**: Driven by database configuration. Values are read from `ServiceProvider` (and Telegence auth) but not changed in this code. To change, update DB config; the code will consume the new values on next run.

---

### 1) Error notifications and sync lifecycle

#### What happens at sync start and end
- The Lambda logs the beginning and end of processing.
  - Start:
    ```text
    LogInfo(keysysContext, "STATUS", $"TelegenceGetDevices::Beginning to process {processedRecordCount} records...");
    ```
  - End:
    ```text
    LogInfo(keysysContext, "STATUS", $"Processed {processedRecordCount} records.");
    ```
- The function posts messages to SQS to advance processing across pages/groups, but this is orchestration, not a user-facing notification.

Key locations:
- `AltaworxTelegenceAWSGetDevices.cs` → `FunctionHandler(...)` and `SendMessageToGet*QueueAsync(...)`

#### Error handling behavior
- Errors are caught and written to logs. Examples:
  - Top-level handler:
    ```text
    catch (Exception ex)
    {
        if (keysysContext is null)
        {
            context.Logger.LogError($"EXCEPTION: {ex.Message} {ex.StackTrace}");
        }
        else
        {
            LogInfo(keysysContext, "EXCEPTION", ex.Message + " " + ex.StackTrace);
        }
    }
    ```
  - Inner processing:
    ```text
    catch (Exception ex)
    {
        LogInfo(context, "EXCEPTION:TelegenceGetDevices:", ex.Message + " " + ex.StackTrace);
    }
    ```

#### What does NOT happen
- There is no call to any outbound notification channel (no SNS topic publish, no SES email, no Slack/MS Teams webhook, etc.).
- A repository-wide search found no usage of SNS/SES/Email APIs in the sync path; only logging and SQS are used.

#### Implications
- If you rely on operational alerts for failures or completes, they must come from:
  - Centralized log monitors/alerts (e.g., CloudWatch alarms on error logs), or
  - Additional notifier code you add at key points (see below).

#### How to add notifications (reference approach)
- Introduce a notifier (SNS/SES/Webhook) and invoke it at:
  - Start of a run (after "Beginning to process...")
  - End of a run (after "Processed ...")
  - In catch blocks for failures and in branches where the sync decides to stop or retry
- Keep it behind a feature flag so it can be enabled per environment/provider.

---

### 2) Billing periods: where they come from and how they change

#### Where billing period values come from
- `ServiceProvider` table (read via `ServiceProviderCommon`):
  - Fields: `BillPeriodEndDay`, `BillPeriodEndHour`, `OptimizationStartHourLocalTime`, `ContinuousLastDayOptimizationStartHourLocalTime`.
  - Code reads these via queries like:
    ```sql
    SELECT Id, Name, DisplayName, IntegrationId, TenantId,
           [BillPeriodEndDay], [BillPeriodEndHour],
           [OptimizationStartHourLocalTime], [ContinuousLastDayOptimizationStartHourLocalTime],
           [WriteIsEnabled], [RegisterCarrierServiceCallBack]
      FROM ServiceProvider
     WHERE Id = @serviceProviderId
    ```
- Telegence authentication record includes a `BillPeriodEndDay` fallback for Telegence:
  - `TelegenceCommon.GetTelegenceAuthenticationInformation(...)` maps `BillPeriodEndDay` from the auth SP result (defaults to 1 when null).
- Optimization instances (not specific to this Lambda) include `BillingPeriodStartDate` and `BillingPeriodEndDate` fields retrieved from DB and surfaced via `AWSFunctionBase.InstanceFromReader(...)` when those services are used.

#### How billing periods change
- This codebase does not compute or mutate billing periods. It only reads the configured values.
- To change a provider’s billing period, update the relevant DB records:
  - For general provider settings: update `ServiceProvider.BillPeriodEndDay` and `ServiceProvider.BillPeriodEndHour` (and related optimization hour fields if applicable).
  - For Telegence-specific fallback: update the Telegence integration authentication record that exposes `BillPeriodEndDay`.
- Once DB values are updated, subsequent runs will pick up the new configuration automatically.

#### Operational considerations when changing billing periods
- Ensure the effective timezone policy is understood; `OptimizationStartHourLocalTime` implies local-time scheduling considerations outside this Lambda.
- Align downstream jobs and reporting windows (e.g., device usage/detail processors, optimizations) to the new `BillPeriodEndDay/Hour` to avoid gaps/overlaps around the transition.
- If you add notifications, consider sending a one-time message when a provider’s billing period config changes.

---

### File pointers (for future reference)
- `AltaworxTelegenceAWSGetDevices.cs`
  - Logging of start/end and exception handling; SQS orchestration.
- `AWSFunctionBase.cs`
  - Shared logging helpers; exposes `BillingPeriodStartDate/EndDate` on optimization instances (when applicable).
- `ServiceProviderCommon.cs`
  - Reads `BillPeriodEndDay`, `BillPeriodEndHour`, and optimization hour fields from `ServiceProvider`.
- `TelegenceCommon.cs`
  - Reads Telegence auth (includes `BillPeriodEndDay` fallback), and makes Telegence API calls.

---

### Summary answers
- **Error notifications – Do we trigger any notifications while starting and ending the Telegence sync? Do we trigger error notifications?**
  - No built-in notifications. The function logs start/end and errors; it does not publish to notification channels.
- **Bill periods – How do the providers’ bill periods change?**
  - Via database configuration. Update `ServiceProvider` (and Telegence auth where relevant); the code consumes the new values on its next run.
