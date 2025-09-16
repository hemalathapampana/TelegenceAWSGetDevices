# TelegenceGetDevices Lambda Analysis

## Overview
The TelegenceGetDevices Lambda is a comprehensive data synchronization service that retrieves device information from the Telegence API and processes it through multiple stages including BAN (Billing Account Number) status validation, device staging, and error handling.

## 1. Lambda Triggers

### **Answer: SQS Queue Triggered**
```csharp:48:48:AltaworxTelegenceAWSGetDevices.cs
public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
```

**Trigger Sources:**
- **SQS Queue**: Primary trigger via `TelegenceDestinationQueueGetDevicesURL`
- **Manual Execution**: Can be invoked manually (when `sqsEvent?.Records` is null)
- **Self-Requeue**: Lambda re-queues itself for continuation processing

**Message Attributes Used:**
- `CurrentPage`: API pagination tracking
- `HasMoreData`: Indicates if more API pages exist
- `CurrentServiceProviderId`: Service provider being processed
- `InitializeProcessing`: Determines processing phase
- `IsProcessDeviceNotExistsStaging`: Device validation phase flag
- `GroupNumber`: Batch processing group identifier
- `RetryNumber`: Retry attempt counter

## 2. SQL Retry Logic and Issue Prevention

### **Answer: SQL Retry prevents transient database failures**
```csharp:280:287:AltaworxTelegenceAWSGetDevices.cs
policyFactory.SqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES).Execute(() =>
{
    deviceCount = SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(context),
    context.CentralDbConnectionString,
    SQLConstant.StoredProcedureName.TELEGENCE_GET_CURRENT_DEVICES_COUNT,
    parameters,
    SQLConstant.ShortTimeoutSeconds);
});
```

**Why SQL Retry is Done First:**
1. **Transient Connection Issues**: Network hiccups, connection pool exhaustion
2. **Database Lock Contention**: Temporary table locks during concurrent operations
3. **Timeout Prevention**: Prevents premature failures on slow queries
4. **Data Consistency**: Ensures critical operations complete successfully

**Issues Prevented:**
- Connection timeouts
- Deadlock scenarios
- Network intermittency
- Resource contention
- Transaction rollbacks

## 3. Staging Table Management

### **Answer: Staging tables are cleared at the start for new service providers**
```csharp:199:201:AltaworxTelegenceAWSGetDevices.cs
TruncateTelegenceDeviceAndUsageStaging(context);
TruncateTelegenceBillingAccountNumberStatusStaging(context);
syncState.CurrentServiceProviderId = serviceProvider;
```

**Staging Tables Cleared:**
- `TelegenceDeviceStaging` - via `usp_Telegence_Truncate_DeviceAndUsageStaging`
- `TelegenceDeviceBillingNumberAccountStatusStaging` - via `usp_Telegence_Truncate_BillingAccountNumberStatusStaging`

**Clearing Logic:**
- **Only for new service providers** (when `syncState.CurrentServiceProviderId == 0`)
- **Not cleared between retry attempts** - data persists for continuation
- **Fresh start per service provider** - ensures clean processing state

### **Answer: Staging tables are NOT cleared after previous runs**
Staging tables persist between runs to support:
- **Continuation processing** across Lambda timeouts
- **Retry mechanisms** without data loss
- **Multi-page API processing** state maintenance
- **Error recovery** scenarios

## 4. BAN, FAN, and Number Status Storage

### **Answer: TelegenceDeviceBillingNumberAccountStatusStaging stores BAN statuses**
```csharp:398:414:AltaworxTelegenceAWSGetDevices.cs
using (var sqlCommand = new SqlCommand("SELECT BillingAccountNumber, Status, ServiceProviderId FROM TelegenceDeviceBillingNumberAccountStatusStaging where BillingAccountNumber IS NOT NULL", connection))
{
    using (var reader = sqlCommand.ExecuteReader())
    {
        while (reader.Read())
        {
            var key = reader[0].ToString();
            if (!banStatuses.ContainsKey(key))
            {
                banStatuses.Add(key, reader[1].ToString());
            }
        }
    }
}
```

**Storage Table:** `TelegenceDeviceBillingNumberAccountStatusStaging`
- **BillingAccountNumber**: BAN identifier
- **Status**: BAN status from Telegence API
- **ServiceProviderId**: Associated service provider

**Device Storage Table:** `TelegenceDeviceStaging`
```csharp:554:563:AltaworxTelegenceAWSGetDevices.cs
table.Columns.Add(CommonColumnNames.Id);
table.Columns.Add(CommonColumnNames.ServiceProviderId);
table.Columns.Add(CommonColumnNames.FoundationAccountNumber); // FAN
table.Columns.Add(CommonColumnNames.BillingAccountNumber);    // BAN
table.Columns.Add(CommonColumnNames.SubscriberNumber);        // Number
table.Columns.Add(CommonColumnNames.SubscriberNumberStatus);  // Number Status
table.Columns.Add(CommonColumnNames.RefreshTimestamp);
table.Columns.Add(CommonColumnNames.CreatedDate);
table.Columns.Add(CommonColumnNames.BanStatus);
```

## 5. BAN List Status Source

### **Answer: BAN statuses are read from TelegenceDeviceBillingNumberAccountStatusStaging**
```csharp:391:415:AltaworxTelegenceAWSGetDevices.cs
private Dictionary<string, string> GetBanListStatusesStaging(string centralDbConnectionString)
{
    Dictionary<string, string> banStatuses = new Dictionary<string, string>();
    using (SqlConnection connection = new SqlConnection(centralDbConnectionString))
    {
        connection.Open();
        using (var sqlCommand = new SqlCommand("SELECT BillingAccountNumber, Status, ServiceProviderId FROM TelegenceDeviceBillingNumberAccountStatusStaging where BillingAccountNumber IS NOT NULL", connection))
```

**BAN Status Flow:**
1. **Initial Population**: Retrieved from Telegence API via `TelegenceBanDetailGetURL`
2. **Staged Storage**: Saved to `TelegenceDeviceBillingNumberAccountStatusStaging`
3. **Processing Use**: Read from staging for device validation
4. **API Source**: `GET /billingaccounts/{ban}` endpoint

## 6. Telegence API Endpoint Details

### **Answer: GetTelegenceDevicesAsync calls the main device list endpoint**
```csharp:535:535:AltaworxTelegenceAWSGetDevices.cs
return await TelegenceCommon.GetTelegenceDevicesAsync(context, syncState, ProxyUrl, telegenceDeviceList, TelegenceDevicesGetURL, BatchSize);
```

**Primary API Endpoints:**
- **Device List**: `TelegenceDevicesGetURL` - Main device retrieval endpoint
- **BAN Detail**: `TelegenceBanDetailGetURL` - BAN status endpoint (pattern: `/billingaccounts/{ban}`)
- **Device Detail**: `TelegenceDeviceDetailGetURL` - Individual device details (pattern: `/devices/{subscriberNumber}`)

**API Call Implementation:**
```csharp:438:470:TelegenceCommon.cs
public static async Task<TelegenceGetDevicesSyncState> GetTelegenceDevicesAsync(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, string proxyUrl,
   List<TelegenceDeviceResponse> telegenceDeviceList, string deviceDetailEndpoint, int pageSize)
```

## 7. API Pagination Configuration

### **Answer: Page size is configurable via BatchSize environment variable (default: 250)**
```csharp:35:36:AltaworxTelegenceAWSGetDevices.cs
private int BatchSize = Convert.ToInt32(Environment.GetEnvironmentVariable("BatchSize")); // 250
private int DEFAULT_BATCH_SIZE = 250;
```

**Pagination Parameters:**
- **Page Size**: `BatchSize` (default 250, configurable via environment)
- **Current Page**: Tracked in `syncState.CurrentPage`
- **Max Cycles**: `MaxCyclesToProcess` (configurable limit per Lambda execution)

**Header Implementation:**
```csharp:547:548:TelegenceCommon.cs
headerContent.Add(CommonConstants.CURRENT_PAGE, syncState.CurrentPage.ToString());
headerContent.Add(CommonConstants.PAGE_SIZE, pageSize);
```

## 8. API Pagination Completion Detection

### **Answer: System uses page-total header and hasMoreData flag**
```csharp:567:572:TelegenceCommon.cs
if (int.TryParse(headers[CommonConstants.PAGE_TOTAL].ToString(), out int pageTotal))
{
    syncState.HasMoreData = syncState.CurrentPage < pageTotal;
}
syncState.IsLastCycle = !syncState.HasMoreData;
```

**Completion Detection Methods:**
1. **Page Total Header**: API returns `page-total` header indicating total pages
2. **HasMoreData Flag**: Calculated as `CurrentPage < PageTotal`
3. **IsLastCycle Flag**: Set when no more data available
4. **Empty Response**: No devices returned indicates completion

## 9. GetTelegenceDeviceBySubscriberNumber Parameters

### **Answer: Uses subscriberNumber and device detail endpoint**
```csharp:627:628:AltaworxTelegenceAWSGetDevices.cs
string resultAPI = await TelegenceCommon.GetTelegenceDeviceBySubscriberNumber(context, telegenceAuthenticationInfo, context.IsProduction,
                            telegenceDevice.SubscriberNumber, TelegenceDeviceDetailGetURL, ProxyUrl);
```

**Parameters Used:**
- **subscriberNumber**: Device identifier (phone number/MSISDN)
- **endpoint**: `TelegenceDeviceDetailGetURL` template
- **isProduction**: Environment flag (production vs sandbox)
- **proxyUrl**: Proxy configuration for external calls

**Endpoint Construction:**
```csharp:362:362:TelegenceCommon.cs
var deviceDetailEndpoint = $"{endpoint}{subscriberNo}";
```

## 10. Device Validation and Failure Handling

### **Answer: Failed devices are logged and excluded from processing**
```csharp:636:651:AltaworxTelegenceAWSGetDevices.cs
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
```

**Validation Rules:**
- **Status Changes**: Only process devices with status changes
- **Cancelled Status**: Ignore devices with status "C" (cancelled)
- **API Failures**: Devices with API failures are skipped but logged

**Failure Handling:**
- **Logging**: Failed API calls logged with error details
- **Continuation**: Processing continues with remaining devices
- **Retry Logic**: Polly retry policy handles transient failures

## 11. Retry Configuration (Polly)

### **Answer: Uses configurable retry attempts with exponential backoff**
```csharp:502:510:TelegenceCommon.cs
var responseMessage = await Amop.Core.Helpers.RetryPolicyHelper.PollyRetryForProxyRequestAsync(context.logger, CommonConstants.NUMBER_OF_TELEGENCE_RETRIES).ExecuteAsync(async () =>
{
    using (var client = new HttpClient())
    {
        ConfigHttpClient(client);
        var responseContent = MappingProxyResponseContent(client.GetWithProxy(proxyUrl, payload, context.logger));
        return await Task.FromResult(responseContent);
    }
});
```

**Retry Configuration:**
- **Attempts**: `CommonConstants.NUMBER_OF_TELEGENCE_RETRIES`
- **Policy Type**: Polly retry with exponential backoff
- **Scope**: Applied to all Telegence API calls
- **Delay**: Progressive delays between attempts

**Lambda-Level Retries:**
```csharp:221:224:AltaworxTelegenceAWSGetDevices.cs
if (remainingBanNeedToProcesses.Count > 0 && syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES)
{
    await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, delaySeconds: CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS);
}
```

## 12. Re-enqueuing for Incomplete Processing

### **Answer: Uses SQS message attributes to track state and re-enqueue**
```csharp:501:503:AltaworxTelegenceAWSGetDevices.cs
if ((!syncState.IsLastCycle || syncState.HasMoreData) && syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES)
{
    await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, delaySeconds: CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS);
}
```

**Re-enqueuing Triggers:**
- **Timeout Conditions**: Lambda approaching time limit
- **Incomplete Pages**: More API pages to process
- **Retry Scenarios**: Failed operations within retry limits
- **Batch Processing**: Group-based device processing

**State Preservation:**
```csharp:905:915:AltaworxTelegenceAWSGetDevices.cs
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
}
```

## 13. Stored Procedures in the Flow

### **Answer: Multiple stored procedures handle different processing phases**

**BAN Processing:**
- `USP_TELEGENCE_DEVICES_PREPARE_BANS_TO_PROCESS` - Prepares BAN list for processing
- `GET_BAN_LIST_NEED_TO_PROCESS` - Retrieves BANs needing status updates
- `MARK_PROCESSED_FOR_BAN` - Marks BANs as processed

**Device Processing:**
- `TELEGENCE_GET_CURRENT_DEVICES_COUNT` - Counts existing devices for service provider
- `GET_TELEGENCE_DEVICES_NOT_EXISTS_ON_STAGING` - Gets devices for existence validation
- `MARK_TELEGENCE_DEVICES_PROCESSED_ON_PROCESS_CHECK_EXISTS_ON_STAGING` - Marks devices as processed
- `usp_GetTelegenDevice_NotExists_Stagging` - Populates device not-exists staging table

**Service Provider Management:**
- `usp_DeviceSync_Get_NextServiceProviderIdByIntegration` - Gets next service provider
- `usp_Telegence_Get_AuthenticationByProviderId` - Retrieves authentication info

**Cleanup Operations:**
- `usp_Telegence_Truncate_DeviceAndUsageStaging` - Clears device staging
- `usp_Telegence_Truncate_BillingAccountNumberStatusStaging` - Clears BAN staging

## 14. Summary Logging Details

### **Answer: Comprehensive logging covers all processing phases**

**Key Log Categories:**
```csharp:77:77:AltaworxTelegenceAWSGetDevices.cs
LogInfo(keysysContext, "STATUS", $"TelegenceGetDevices::Beginning to process {processedRecordCount} records...");
```

**Logged Information:**
- **Processing Status**: Record counts, phase transitions
- **API Interactions**: Request URLs, response status, retry attempts
- **Database Operations**: SQL execution, row counts, stored procedure calls
- **Error Conditions**: Exceptions, failures, timeout scenarios
- **State Tracking**: Page numbers, service providers, retry counts
- **Performance Metrics**: Processing times, batch sizes

**Log Examples:**
- Record processing counts
- API endpoint calls and responses
- SQL bulk copy operations
- Service provider transitions
- Device validation results
- Queue message sending confirmations

## 15. Reference Items Usage

### **Answer: Functions, queues, and procedures work together in orchestrated flow**

**Functions (Lambdas):**
- **TelegenceGetDevices**: Main orchestrator
- **TelegenceGetDeviceUsage**: Triggered after device processing
- **TelegenceGetDeviceDetail**: Triggered for detailed device information

**Queues:**
- **TelegenceDestinationQueueGetDevicesURL**: Self-continuation queue
- **TelegenceDeviceUsageQueueURL**: Device usage processing queue
- **TelegenceDeviceDetailQueueURL**: Device detail processing queue

**Processing Flow:**
1. **Device List Processing** → Self-requeue for continuation
2. **Device Completion** → Trigger Usage and Detail queues
3. **Error Scenarios** → Retry mechanisms and error queues

**Queue Message Flow:**
```csharp:517:517:AltaworxTelegenceAWSGetDevices.cs
await SendMessageToGetDeviceUsageQueueAsync(context, TelegenceDeviceUsageQueueURL, 5);
```

## 16. Multi-Carrier Support

### **Answer: Architecture supports multiple carriers through service provider abstraction**

**Service Provider Management:**
```csharp:187:203:AltaworxTelegenceAWSGetDevices.cs
var serviceProvider = ServiceProviderCommon.GetNextServiceProviderId(context.CentralDbConnectionString, Amop.Core.Models.IntegrationType.Telegence, syncState.CurrentServiceProviderId);
switch (serviceProvider)
{
    case 0: /* Exception */
        LogInfo(context, "WARNING", $"Error Getting a service provider id for Telegence CurrentServiceProviderId: {syncState.CurrentServiceProviderId}");
        proceed = false;
        break;
    case -1:
        LogInfo(context, "WARNING", "No Authentication record was found for Telegence Service Provider.");
        proceed = false;
        break;
    default:
        TruncateTelegenceDeviceAndUsageStaging(context);
        TruncateTelegenceBillingAccountNumberStatusStaging(context);
        syncState.CurrentServiceProviderId = serviceProvider;
        break;
}
```

**Multi-Carrier Features:**
- **Service Provider Iteration**: Processes multiple carriers sequentially
- **Authentication Per Provider**: Separate credentials per carrier
- **Staging Isolation**: Clean staging per service provider
- **Configuration Flexibility**: Environment variables per deployment
- **FAN Filtering**: Include/exclude FAN lists per provider

**Carrier Processing Flow:**
1. Process first service provider completely
2. Move to next service provider when current completes
3. Clean staging tables between providers
4. Maintain separate authentication contexts
5. Apply provider-specific filtering rules

---

## Summary

The TelegenceGetDevices Lambda is a sophisticated, multi-phase data synchronization system that:

1. **Orchestrates** complex API interactions with retry resilience
2. **Manages** multi-carrier processing with proper isolation
3. **Handles** large-scale pagination and timeout scenarios
4. **Provides** comprehensive error handling and logging
5. **Supports** continuation processing across Lambda timeouts
6. **Maintains** data consistency through staging table management
7. **Integrates** with downstream processing through queue messaging

The system is designed for reliability, scalability, and comprehensive carrier data synchronization with robust error recovery mechanisms.