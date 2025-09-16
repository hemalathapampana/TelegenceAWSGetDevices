# Telegence Lambda Detailed Analysis

## Overview
This document provides a comprehensive analysis of the Telegence AWS Lambda function (`AltaworxTelegenceAWSGetDevices`) that handles device synchronization with the Telegence API. The analysis covers all aspects requested including triggers, retry mechanisms, staging tables, API details, business rules, stored procedures, and error handling.

## 1. SQS Event Triggers

### What Generates the SQS Events?
The SQS events that trigger this Lambda function are **self-generated** through recursive messaging patterns:

1. **Initial Trigger**: The Lambda can be invoked either:
   - Manually (no SQS event - `sqsEvent?.Records` is null)
   - Via SQS message with specific message attributes

2. **Self-Enqueuing Pattern**: The Lambda uses several methods to enqueue messages to itself:
   - `SendMessageToGetDevicesQueueAsync()` - Continues device processing
   - `SendMessageToGetDeviceUsageQueueAsync()` - Triggers device usage processing
   - `SendMessageToGetDeviceDetailQueueAsync()` - Triggers device detail processing

3. **Queue URLs Used**:
   - `TelegenceDestinationQueueGetDevicesURL` - Main processing queue
   - `TelegenceDeviceUsageQueueURL` - Device usage processing
   - `TelegenceDeviceDetailQueueURL` - Device detail processing

### Message Attributes in SQS Events:
- `CurrentPage` - Current pagination page
- `HasMoreData` - Boolean indicating more data available
- `CurrentServiceProviderId` - Service provider being processed
- `InitializeProcessing` - Boolean for initialization phase
- `IsProcessDeviceNotExistsStaging` - Flag for processing missing devices
- `GroupNumber` - Batch group identifier
- `IsLastProcessDeviceNotExistsStaging` - Flag for final processing batch
- `RetryNumber` - Current retry attempt count

## 2. Retry Initialization

### Why SQL Retry is First Step in Initialization
SQL retry is implemented as the very first step to protect against:

1. **Database Connection Issues**: Transient network or connection problems
2. **SQL Server Deadlocks**: Common in high-concurrency environments
3. **Timeout Issues**: Long-running queries that may timeout intermittently
4. **Resource Contention**: When multiple Lambda instances access the same database

### Implementation Details:
```csharp
var policyFactory = new PolicyFactory(context.logger);
policyFactory.SqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES).Execute(() => {
    // SQL operations here
});
```

The retry policy uses **Polly** library with:
- **Retry Count**: `CommonConstants.NUMBER_OF_RETRIES` attempts
- **Applied to**: All stored procedure executions
- **Scope**: Database operations throughout the entire Lambda execution

## 3. Clearing Staging Tables

### Staging Tables Cleared at Start:
Yes, the staging tables are **definitely cleared** at the start of each run:

1. **Device and Usage Staging**: 
   - **Method**: `TruncateTelegenceDeviceAndUsageStaging()`
   - **Stored Procedure**: `usp_Telegence_Truncate_DeviceAndUsageStaging`
   - **When**: When `CurrentServiceProviderId == 0` (initial processing)

2. **BAN Status Staging**:
   - **Method**: `TruncateTelegenceBillingAccountNumberStatusStaging()`
   - **Stored Procedure**: `usp_Telegence_Truncate_BillingAccountNumberStatusStaging`
   - **When**: When `CurrentServiceProviderId == 0` (initial processing)

### Why Manual Clearing is Necessary:
The staging tables are **NOT automatically cleared** after previous runs because:
- They serve as intermediate storage for multi-step processing
- Failed runs might leave partial data that needs to be cleared
- Multiple service providers may be processed in sequence
- The system needs to ensure clean state for each processing cycle

## 4. BAN/FAN Status Storage

### Storage Location for BAN, FAN, and Number Statuses:
**Primary Table**: `TelegenceDeviceBillingNumberAccountStatusStaging`

**Table Structure** (inferred from code):
```sql
TelegenceDeviceBillingNumberAccountStatusStaging
├── Id (Identity)
├── BillingAccountNumber (varchar)
├── Status (varchar) 
└── ServiceProviderId (int)
```

**Storage Process**:
1. **Fetch**: BAN statuses retrieved via `TelegenceCommon.GetBanStatusAsync()`
2. **API Endpoint**: `TelegenceBanDetailGetURL` with BAN parameter
3. **Bulk Insert**: Using `SqlBulkCopy()` method
4. **Method**: `SaveBillingAccountNumberStatusStaging()`

**No Additional Tables**: The code shows only this single staging table for BAN/FAN status storage.

## 5. BAN List Retrieval

### Source of BAN List in Normal Flow:
In the "Normal Flow," BAN list statuses are retrieved **directly from the staging table**:

**Method**: `GetBanListStatusesStaging()`
**Query**: 
```sql
SELECT BillingAccountNumber, Status, ServiceProviderId 
FROM TelegenceDeviceBillingNumberAccountStatusStaging 
WHERE BillingAccountNumber IS NOT NULL
```

**Process Flow**:
1. **Initialization Phase**: Populates staging table from Telegence API
2. **Normal Flow**: Reads from staging table (not from API again)
3. **Source**: `TelegenceDeviceBillingNumberAccountStatusStaging` table

## 6. API Details – Device Fetch

### Exact Telegence API Endpoint:
**Method**: `TelegenceCommon.GetTelegenceDevicesAsync()`
**URL**: Environment variable `TelegenceDevicesGetURL`
**Base URL**: 
- Production: `telegenceAuth.ProductionUrl`
- Sandbox: `telegenceAuth.SandboxUrl`

### Page Size/Limit Configuration:
**Page Size**: Environment variable `BatchSize` (default: 250)
**Configuration**:
```csharp
private int BatchSize = Convert.ToInt32(Environment.GetEnvironmentVariable("BatchSize")); // 250
private int DEFAULT_BATCH_SIZE = 250;
```

### Headers Used:
- `app-id`: `telegenceAuth.ClientId`
- `app-secret`: `telegenceAuth.ClientSecret`
- `currentPage`: Current page number
- `pageSize`: Batch size value

### Pagination Completion Detection:
**Method**: Response header analysis
```csharp
if (int.TryParse(responseMessage.Headers.GetValues(CommonConstants.PAGE_TOTAL).FirstOrDefault(), out int pageTotal))
{
    syncState.HasMoreData = syncState.CurrentPage < pageTotal;
}
syncState.IsLastCycle = !syncState.HasMoreData;
```

**Indicators**:
- `PAGE_TOTAL` header indicates total pages
- `syncState.HasMoreData = syncState.CurrentPage < pageTotal`
- `IsLastCycle` flag set when no more data

### Max Cycles Protection:
**Failsafe**: `MaxCyclesToProcess` environment variable
**Purpose**: Prevents infinite loops if API pagination fails
```csharp
while (cycleCounter <= MaxCyclesToProcess)
```

## 7. Missing Devices Handling

### API Details for Subscriber-Level Validation:
**Method**: `TelegenceCommon.GetTelegenceDeviceBySubscriberNumber()`
**Endpoint**: `TelegenceDeviceDetailGetURL + subscriberNumber`
**Parameters**:
- `subscriberNo`: Individual subscriber number
- `endpoint`: Device detail API endpoint
- `proxyUrl`: Proxy configuration if applicable

### API Call Structure:
```csharp
string resultAPI = await TelegenceCommon.GetTelegenceDeviceBySubscriberNumber(
    context, 
    telegenceAuthenticationInfo, 
    context.IsProduction,
    telegenceDevice.SubscriberNumber, 
    TelegenceDeviceDetailGetURL, 
    ProxyUrl
);
```

### Failed Device Validation Handling:
**What Happens to Failed Devices**:

1. **Logging**: Failures are logged but devices are not discarded
2. **Status Check**: System checks if `subscriberStatus != CANCEL_STATUS ("C")`
3. **Conditional Processing**: Only devices with status changes are processed
4. **Retry Logic**: Uses same retry mechanism as other API calls
5. **Marking Processed**: Devices are marked as processed regardless of API success:
   ```csharp
   MarkProcessForEachDevicesHaveProcessed(ParameterizedLog(context), 
       context.CentralDbConnectionString, listDevicesProcessed, policyFactory);
   ```

**Business Rule**: Devices with status "C" (cancelled) are ignored as they won't appear in the main device list API.

## 8. Error Handling / Retry Configuration

### Polly Configuration Details:
**SQL Retry Policy**:
- **Attempts**: `CommonConstants.NUMBER_OF_RETRIES`
- **Type**: Synchronous execution
- **Scope**: All stored procedure calls

**HTTP Retry Policy**:
- **Attempts**: `CommonConstants.NUMBER_OF_TELEGENCE_RETRIES`
- **Methods**: 
  - `PollyRetryHttpRequestAsync()` - Direct HTTP calls
  - `PollyRetryForProxyRequestAsync()` - Proxy-based calls
- **Scope**: All Telegence API calls

### Lambda Retry Configuration:
**Lambda-Level Retries**: `CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES`
**Applied To**:
- BAN processing retries
- Device processing continuation
- Device validation processing

### Re-enqueuing Messages Technical Implementation:
**How It Works**:
1. **New SQS Message Creation**: Yes, creates entirely new SQS messages
2. **Message Attributes**: Carries forward state information
3. **Delay Mechanism**: Uses `DelaySeconds` parameter
4. **State Preservation**: Current page, service provider, retry count, etc.

**Example**:
```csharp
await SendMessageToGetDevicesQueueAsync(context, syncState, 
    TelegenceDestinationQueueGetDevicesURL, 
    delaySeconds: CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS);
```

**Not Retry of Existing Message**: Creates new message with updated state, not retry of original.

## 9. Business Rules Details

### Service Provider Processing:
1. **Sequential Processing**: One service provider at a time
2. **Service Provider Discovery**: `ServiceProviderCommon.GetNextServiceProviderId()`
3. **Integration Type Filter**: Only Telegence integration type
4. **Continuation Logic**: Automatically moves to next service provider when current is complete

### Device Filtering (FAN Filter):
**Configuration Source**: `ServiceProviderSetting` table
**Filter Types**:
- `IncludedFANs`: Only process devices with these Foundation Account Numbers
- `ExcludedFANs`: Skip devices with these Foundation Account Numbers

**Implementation**:
```csharp
if (includedFANs.Count > 0)
{
    filteredTelegenceDeviceList = filteredTelegenceDeviceList
        .Where(x => includedFANs.Contains(x.FoundationAccountNumber)).ToList();
}
if (excludedFANs.Count > 0)
{
    filteredTelegenceDeviceList = filteredTelegenceDeviceList
        .Where(x => !excludedFANs.Contains(x.FoundationAccountNumber)).ToList();
}
```

### Time-Based Processing Rules:
**Timeout Protection**: `CommonConstants.REMAINING_TIME_CUT_OFF`
**Behavior**: 
- Stops processing when Lambda timeout approaches
- Increments retry counter
- Re-enqueues message for continuation

### First Sync Detection:
**Logic**: Checks existing device count for service provider
```csharp
var existingDeviceCount = GetTelegenceCurrentDeviceCount(context, syncState.CurrentServiceProviderId, policyFactory);
if (existingDeviceCount == 0) // First sync
```
**Special Handling**: First sync gets BAN status from API instead of staging table

### Device Status Business Rules:
1. **Cancel Status Exclusion**: Devices with status "C" (cancelled) are ignored
2. **Status Change Detection**: Only processes devices where API status differs from stored status
3. **Unknown Status Handling**: Devices with "Unknown" status are processed differently

## 10. Stored Procedures Functionality

### Core Stored Procedures and Usage:

#### Service Provider Management:
**`usp_DeviceSync_Get_NextServiceProviderIdByIntegration`**
- **Purpose**: Gets next service provider for Telegence integration
- **Parameters**: `@providerId`, `@integrationId`
- **Usage**: Sequential service provider processing
- **Location**: `ServiceProviderCommon.GetNextServiceProviderId()`

#### Authentication:
**`usp_Telegence_Get_AuthenticationByProviderId`**
- **Purpose**: Retrieves Telegence API credentials for service provider
- **Parameters**: `@providerId`
- **Returns**: ClientId, ClientSecret, URLs, credentials
- **Usage**: API authentication setup

#### BAN Processing:
**`USP_TELEGENCE_DEVICES_PREPARE_BANS_TO_PROCESS`**
- **Purpose**: Prepares billing account numbers for processing
- **Usage**: Called before BAN status retrieval
- **Location**: `PrepareBanListToProcess()`

**`GET_BAN_LIST_NEED_TO_PROCESS`**
- **Purpose**: Gets list of BANs that need status checking
- **Returns**: List of billing account numbers
- **Usage**: Drives BAN status API calls

**`MARK_PROCESSED_FOR_BAN`**
- **Purpose**: Marks BANs as processed after status retrieval
- **Parameters**: Comma-separated BAN list
- **Usage**: Prevents reprocessing same BANs

#### Device Processing:
**`TELEGENCE_GET_CURRENT_DEVICES_COUNT`**
- **Purpose**: Gets current device count for service provider
- **Parameters**: `@SERVICE_PROVIDER_ID`
- **Usage**: First sync detection logic

**`GET_TELEGENCE_DEVICES_NOT_EXISTS_ON_STAGING`**
- **Purpose**: Gets devices that exist in AMOP but not in API staging
- **Parameters**: `@GROUP_NUMBER`
- **Usage**: Identifies devices for individual validation

**`usp_GetTelegenDevice_NotExists_Stagging`**
- **Purpose**: Populates staging table with devices needing validation
- **Parameters**: `@BatchSize`
- **Usage**: Batch processing setup for device validation

#### Cleanup Operations:
**`usp_Telegence_Truncate_DeviceAndUsageStaging`**
- **Purpose**: Clears device and usage staging tables
- **Usage**: Run initialization cleanup

**`usp_Telegence_Truncate_BillingAccountNumberStatusStaging`**
- **Purpose**: Clears BAN status staging table  
- **Usage**: Run initialization cleanup

#### Processing Tracking:
**`MARK_TELEGENCE_DEVICES_PROCESSED_ON_PROCESS_CHECK_EXISTS_ON_STAGING`**
- **Purpose**: Marks devices as processed during validation phase
- **Parameters**: Comma-separated subscriber numbers
- **Usage**: Prevents reprocessing validated devices

## 11. Summary Logs Details

### Logging Infrastructure:
**Base Method**: `AwsFunctionBase.LogInfo()`
**Context**: Uses `KeySysLambdaContext` for structured logging
**Format**: Includes caller file, line number, and function name

### Log Categories and Details:

#### Status Logs:
- `"STATUS"` - Major processing milestones
- `"SUB"` - Sub-process entry points  
- `"INFO"` - General information
- `"WARNING"` - Non-critical issues
- `"ERROR"` - Error conditions
- `"EXCEPTION"` - Exception details with stack traces

#### Processing Metrics:
- **Record Counts**: `"Processed {processedRecordCount} records"`
- **Device Counts**: `"Has {telegenceDevicesFromProcess.Count} Need Check API"`
- **Page Information**: Current page, max cycles, has more data
- **Timing**: Remaining Lambda execution time
- **Group Processing**: Group numbers and batch information

#### API Call Logging:
- **Request URLs**: Full endpoint URLs being called
- **Response Status**: HTTP status codes and success/failure
- **Authentication**: Client IDs (secrets are not logged)
- **Proxy Usage**: Whether proxy is being used
- **Retry Attempts**: Current retry numbers

#### Database Operation Logging:
- **SQL Bulk Copy**: Start/end of bulk insert operations
- **Stored Procedure Execution**: SP names and parameters
- **Row Counts**: Affected rows from database operations
- **Connection Strings**: Database connection information (sanitized)

#### Business Logic Logging:
- **Service Provider**: Current service provider being processed
- **BAN Processing**: BAN numbers and statuses
- **Device Validation**: Subscriber numbers being validated
- **Filter Application**: FAN include/exclude filter details

### Summary Log Examples:
```
STATUS: TelegenceGetDevices::Beginning to process 1 records...
SUB: TryProcessDeviceListAsync: Get Telegence Billing Account Number Status
CurrentPage: 1
MaxCyclesToProcess: 10
HasMoreData: true
INFO: END ProcessDeviceNotExistsStagingAsync: has 15 added TelegenceDeviceStaging
STATUS: Processed 1 records.
```

## 12. Reference Items Usage Context

### Functions:
- **`TryProcessDeviceListAsync()`**: Main orchestration function
- **`ProcessBanListAsync()`**: Handles BAN status retrieval from API
- **`ProcessDeviceListAsync()`**: Handles device list processing and pagination
- **`ProcessDeviceNotExistsStagingAsync()`**: Validates devices missing from API
- **`CheckIfServiceProviderFirstSync()`**: Determines if this is first sync for provider

### Queues:
- **`TelegenceDestinationQueueGetDevicesURL`**: Self-referencing queue for continuation
- **`TelegenceDeviceUsageQueueURL`**: Triggers usage processing Lambda
- **`TelegenceDeviceDetailQueueURL`**: Triggers detail processing Lambda

### Stored Procedures Usage Context:
All stored procedures listed above are used within retry policies and serve specific business functions in the device synchronization workflow.

## 13. Detailed Process Flow for All Carriers

### Overall Architecture:
The Lambda processes **all Telegence service providers sequentially**, not carrier-specific processing. The flow is:

### Phase 1: Initialization
1. **Service Provider Discovery**: Get first/next service provider with Telegence integration
2. **Staging Cleanup**: Truncate staging tables for clean start
3. **BAN Status Processing**: 
   - Prepare BANs for processing
   - Retrieve BAN statuses from Telegence API
   - Store in `TelegenceDeviceBillingNumberAccountStatusStaging`
   - Mark BANs as processed

### Phase 2: Device List Processing
1. **API Pagination**: Retrieve devices from Telegence API in batches
2. **FAN Filtering**: Apply include/exclude filters based on service provider settings
3. **Staging Storage**: Store devices in `TelegenceDeviceStaging`
4. **Continuation Logic**: 
   - If more pages: re-enqueue with next page
   - If complete: move to Phase 3

### Phase 3: Missing Device Validation
1. **Identification**: Find devices in AMOP but not in API staging
2. **Grouping**: Batch devices into groups for processing
3. **Individual Validation**: Call device detail API for each missing device
4. **Status Comparison**: Compare API status with stored status
5. **Conditional Storage**: Only store devices with status changes
6. **Completion**: When all groups processed, trigger usage and detail processing

### Phase 4: Next Service Provider
1. **Provider Transition**: Get next service provider
2. **Loop**: Return to Phase 1 for next provider
3. **Termination**: When no more providers, process ends

### Multi-Carrier Support:
- **Configuration-Driven**: Each service provider has own Telegence authentication
- **Sequential Processing**: One provider at a time to avoid conflicts
- **State Preservation**: SQS messages carry provider-specific state
- **Independent Filtering**: Each provider can have different FAN filters

This architecture ensures **all configured Telegence service providers** are processed systematically with proper error handling, retry logic, and state management throughout the entire workflow.

## Conclusion

This Lambda implements a sophisticated device synchronization system with comprehensive error handling, retry mechanisms, and state management. The self-enqueuing SQS pattern allows for reliable processing of large datasets while respecting Lambda execution time limits. The multi-phase approach ensures data consistency and handles edge cases like missing devices and API failures gracefully.