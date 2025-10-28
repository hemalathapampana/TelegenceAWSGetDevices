## GetDevices Lambda sync logic: where the retention, retry (3 attempts), and Unknown handling occur

### Files reviewed
- `AltaworxTelegenceAWSGetDevices.cs`
- `TelegenceCommon.cs`
- `AWSFunctionBase.cs`
- `ServiceProviderCommon.cs`

### Where the logic lives

- **Entry and sync-state initialization (GetDevices Lambda handler)**
```84:91:/workspace/AltaworxTelegenceAWSGetDevices.cs
var syncState = new TelegenceGetDevicesSyncState();
if (record.MessageAttributes.ContainsKey("CurrentPage"))
{
    syncState.CurrentPage = int.Parse(record.MessageAttributes["CurrentPage"].StringValue);
    LogInfo(keysysContext, "CurrentPage", syncState.CurrentPage);
}
```
```141:142:/workspace/AltaworxTelegenceAWSGetDevices.cs
await TryProcessDeviceListAsync(keysysContext, syncState);
```

- **Initial BAN status pull and retry orchestration (attempts limited by a constant)**
```215:231:/workspace/AltaworxTelegenceAWSGetDevices.cs
if (syncState.InitializeProcessing)
{
    LogInfo(context, "SUB: TryProcessDeviceListAsync", "Get Telegence Billing Account Number Status");
    var banStatus = await ProcessBanListAsync(context, syncState, policyFactory);
    SaveBillingAccountNumberStatusStaging(context, banStatus, syncState.CurrentServiceProviderId);
    var remainingBanNeedToProcesses = GetBanList(ParameterizedLog(context), context.CentralDbConnectionString, policyFactory);
    if (remainingBanNeedToProcesses.Count > 0 && syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES)
    {
        await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, delaySeconds: CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS);
    }
    else
    {
        syncState.InitializeProcessing = false;
        syncState.RetryNumber = 0;
        await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, delaySeconds: CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS);
    }
}
```

- **Paged device list retrieval and time-bound retry (increments `RetryNumber`)**
```450:476:/workspace/AltaworxTelegenceAWSGetDevices.cs
syncState.IsLastCycle = false;
int cycleCounter = 1;
var telegenceDeviceList = new List<TelegenceDeviceResponse>();
while (cycleCounter <= MaxCyclesToProcess)
{
    if (context.Context.RemainingTime.TotalSeconds > CommonConstants.REMAINING_TIME_CUT_OFF)
    {
        if (!syncState.IsLastCycle)
        {
            syncState = await GetTelegenceDevicesFromAPI(context, syncState, telegenceDeviceList);
            syncState.CurrentPage++;
            cycleCounter++;
        }
        else
        {
            break;
        }
    }
    else
    {
        syncState.RetryNumber++;
        LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.STOP_GETTING_SINCE_IS_NOT_ENOUGH_TIME, "Telegence Devices"));
        break;
    }
}
```

- **Re-enqueue while under max attempts; else move "missing" devices to NotExists staging**
```501:512:/workspace/AltaworxTelegenceAWSGetDevices.cs
if ((!syncState.IsLastCycle || syncState.HasMoreData) && syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES)
{
    await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, delaySeconds: CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS);
}
else
{
    // get device exists in AMOP not exists in Api and status != 'UnKnown'. Then moving them to [TelegenceDeviceNotExistsStagingToProcess]
    GetTelegenceDeviceNotExistsStaging(context);
    var maxGroup = GetGroupCount(context);
    syncState.RetryNumber = 0;
    await SendProcessDeviceNotExistsStagingMessagesToQueueAsync(context, syncState, maxGroup);
}
```

- **The "Unknown" status in this repo**
  - The only direct reference is the comment shown above (filtering out devices already marked `Unknown` from the NotExists pipeline). There is no code here that sets a device status to `Unknown`; that transition is likely applied in downstream SQL stored procedures or a separate job after staging.

- **Populate NotExists staging (DB-side logic via stored procedure)**
```801:812:/workspace/AltaworxTelegenceAWSGetDevices.cs
using (var cmd = new SqlCommand("usp_GetTelegenDevice_NotExists_Stagging", con)
{
    CommandType = CommandType.StoredProcedure
})
{
    cmd.CommandTimeout = 800;
    con.Open();
    cmd.Parameters.AddWithValue("@BatchSize", BatchSize);
    cmd.ExecuteNonQuery();
}
```

- **Verify missing devices by subscriber detail; stage updates if status changed and not Cancel ('C')**
```619:651:/workspace/AltaworxTelegenceAWSGetDevices.cs
var telegenceAuthenticationInfo = TelegenceCommon.GetTelegenceAuthenticationInformation(context.CentralDbConnectionString, syncState.CurrentServiceProviderId);
string resultAPI = await TelegenceCommon.GetTelegenceDeviceBySubscriberNumber(context, telegenceAuthenticationInfo, context.IsProduction,
            telegenceDevice.SubscriberNumber, TelegenceDeviceDetailGetURL, ProxyUrl);
if (!string.IsNullOrWhiteSpace(resultAPI))
{
    var deviceDetail = JsonConvert.DeserializeObject<TelegenceMobilityLineConfigurationResponse>(resultAPI);
    var subscriberStatus = deviceDetail.serviceCharacteristic.Where(x => x.Name == SUBSCRIBER_STATUS).Select(x => x.Value).FirstOrDefault();
    if (!string.IsNullOrEmpty(subscriberStatus))
    {
        // if device status is "C" => ignore, b/c Device List Api not return status "C"
        if (telegenceDevice.SubscriberNumberStatus != subscriberStatus && subscriberStatus != CANCEL_STATUS)
        {
            var banStatusText = GetBanStatusTextForDevice(banStatus, telegenceDevice);
            var deviceAdd = new TelegenceDeviceResponse()
            {
                BillingAccountNumber = telegenceDevice.BillingAccountNumber,
                FoundationAccountNumber = telegenceDevice.FoundationAccountNumber,
                SubscriberNumber = telegenceDevice.SubscriberNumber,
                SubscriberNumberStatus = subscriberStatus,
                RefreshTimestamp = telegenceDevice.RefreshTimestamp,
            };
            var dr = AddToDataRow(context, table, telegenceDevice, banStatusText, syncState.CurrentServiceProviderId);
            table.Rows.Add(dr);
        }
    }
}
```

- **NotExists processing also re-enqueues while under max attempts**
```676:684:/workspace/AltaworxTelegenceAWSGetDevices.cs
if (remainingDevicesNeedToProcesses.Count > 0 && syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES)
{
    var isLastProcessDeviceNotExistsStaging = 0;
    if (syncState.IsLastProcessDeviceNotExistsStaging)
    {
        isLastProcessDeviceNotExistsStaging = 1;
    }
    await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS, syncState.GroupNumber, 1, isLastProcessDeviceNotExistsStaging);
}
```

- **SQS requeue message contains `RetryNumber` and control flags**
```904:915:/workspace/AltaworxTelegenceAWSGetDevices.cs
var request = new SendMessageRequest
{
    DelaySeconds = delaySeconds,
    MessageAttributes = new Dictionary<string, MessageAttributeValue>
    {
        {"HasMoreData", new MessageAttributeValue {DataType = "String", StringValue = syncState.HasMoreData ? "true" : "false"}},
        {"CurrentPage", new MessageAttributeValue {DataType = "String", StringValue = syncState.CurrentPage.ToString()}},
        {"CurrentServiceProviderId", new MessageAttributeValue {DataType = "String", StringValue = syncState.CurrentServiceProviderId.ToString()}},
        {"InitializeProcessing", new MessageAttributeValue {DataType = "String", StringValue = syncState.InitializeProcessing.ToString()}},
        {"IsProcessDeviceNotExistsStaging", new MessageAttributeValue {DataType = "String", StringValue = isProcessDeviceNotExistsStaging.ToString()}},
        {"GroupNumber", new MessageAttributeValue {DataType = "String", StringValue = groupNumber.ToString()}},
        {"IsLastProcessDeviceNotExistsStaging", new MessageAttributeValue {DataType = "String", StringValue = isLastGroup.ToString()}},
        {"RetryNumber", new MessageAttributeValue {DataType = "String", StringValue = syncState.RetryNumber.ToString()}}
    },
    MessageBody = "Continuing processing of Telegence devices",
    QueueUrl = telegenceDestinationQueueGetDevicesURL
};
```

- **Device list API pagination and refresh timestamp propagation**
```604:615:/workspace/TelegenceCommon.cs
if (responseMessage.IsSuccessStatusCode)
{
    var deviceList = JsonConvert.DeserializeObject<List<TelegenceDeviceResponse>>(responseBody);
    if (int.TryParse(responseMessage.Headers.GetValues(CommonConstants.PAGE_TOTAL).FirstOrDefault(), out int pageTotal))
    {
        syncState.HasMoreData = syncState.CurrentPage < pageTotal;
    }
    syncState.IsLastCycle = !syncState.HasMoreData;
    if (DateTime.TryParse(responseMessage.Headers.GetValues(CommonConstants.REFRESH_TIMESTAMP).FirstOrDefault(), out DateTime refreshTimestamp))
    {
        GetTelegenceDeviceList(deviceList, telegenceDeviceList, syncState, refreshTimestamp);
    }
}
```

### How the described behavior maps to this code
- **Retain previous device data when not received in daily sync**: This Lambda does not immediately change or purge devices missing from the carrier list. Instead it moves them to `TelegenceDeviceNotExistsStagingToProcess` after pagination completes or retries are exhausted, then re-checks each via the detail API before staging any updates. This effectively preserves existing device state until confirmation.
- **Three consecutive checks**: Re-enqueue decisions are consistently guarded by `syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES` in multiple stages (BAN list processing, device list pagination, NotExists processing). The exact constant value (commonly 3) is defined in `Amop.Core.Constants` outside this repo.
- **Update to "Unknown" after attempts**: There is no code here that sets a device to `Unknown`. The only reference is a filter to skip devices already marked `Unknown` when building the NotExists staging. The `Unknown` transition likely occurs in database stored procedures or a separate post-staging job.

### Key constants and external responsibilities
- **`CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES`**: Controls the attempt limit; defined in shared `Amop.Core.Constants` (not in this repo).
- **Stored procedures and tables** used by this Lambda:
  - `usp_GetTelegenDevice_NotExists_Stagging`
  - `GET_TELEGENCE_DEVICES_NOT_EXISTS_ON_STAGING`
  - `MARK_TELEGENCE_DEVICES_PROCESSED_ON_PROCESS_CHECK_EXISTS_ON_STAGING`
  - Staging tables: `TelegenceDeviceStaging`, `TelegenceDeviceNotExistsStagingToProcess`, `TelegenceDeviceBillingNumberAccountStatusStaging`
  - These likely implement promotion/merging and any `Unknown` status transitions.

### Observations tied to the incident (Suspended at carrier, Active in AMOP)
- If a suspended SIM is omitted from the device list, it enters the NotExists path and is re-checked by the device detail API. If the detail API returns a non-`C` status different from AMOP’s current value, the new status is staged for update. However, if the `Unknown` transition or the final merge is deferred or filtered DB-side, AMOP may continue to show prior "Active".
- Devices already tagged `Unknown` are explicitly skipped by the NotExists fetch comment, which may prevent them from being revisited by this Lambda.

### Potential alternative approaches
- **Revisit `Unknown` gating**: Allow devices already marked `Unknown` to be re-checked periodically via the NotExists path, rather than skipping them.
- **Earlier state transition for missing devices**: On exhausting page retries, optionally set a soft state (e.g., `Unknown`) immediately, then revert if the detail check confirms a status.
- **Tune attempt thresholds**: Reduce `NUMBER_OF_TELEGENCE_LAMBDA_RETRIES` or make it per-provider configurable to shorten the window where stale states persist.
- **Explicit suspended mapping**: Ensure the subscriber detail status values that represent suspension are mapped and promoted reliably from staging, even if the device is excluded from the list endpoint.
- **DB-side SLA**: Apply a DB job that sets `Unknown` if `lastSeenAtCarrier` exceeds an SLA window, independent of Lambda retries.

### Pointers for verification
- Confirm the value of `CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES` (expected 3) in `Amop.Core.Constants`.
- Review the stored procedures listed above to locate where `Unknown` is set and whether devices in `Unknown` are excluded from re-processing.
- Validate the mapping of carrier subscriber statuses (e.g., suspended) into AMOP statuses during the staging-to-main merge.
