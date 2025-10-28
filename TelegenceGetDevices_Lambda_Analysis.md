# TelegenceGetDevices Lambda Analysis - Complete System Documentation

## Overview
The TelegenceGetDevices Lambda is a comprehensive data synchronization service that retrieves device information from the Telegence Carrier API and processes it through multiple stages including BAN (Billing Account Number) status validation, device staging, and error handling.

## Table of Contents
1. [Lambda Triggers and SQS Queue Mechanisms](#1-lambda-triggers-and-sqs-queue-mechanisms)
2. [SQL Retry Logic and Issue Prevention](#2-sql-retry-logic-and-issue-prevention)
3. [Staging Table Management](#3-staging-table-management)
4. [BAN, FAN, and Number Status Storage](#4-ban-fan-and-number-status-storage)
5. [Telegence API Endpoint Details](#5-telegence-api-endpoint-details)
6. [API Pagination Configuration and Completion Detection](#6-api-pagination-configuration-and-completion-detection)
7. [Device Validation and Failure Handling](#7-device-validation-and-failure-handling)
8. [Retry Configuration using Polly](#8-retry-configuration-using-polly)
9. [Re-enqueuing for Incomplete Processing](#9-re-enqueuing-for-incomplete-processing)
10. [Stored Procedures in the Flow](#10-stored-procedures-in-the-flow)
11. [Summary Logging Details](#11-summary-logging-details)
12. [System Orchestration](#12-system-orchestration)

---

## 1. Lambda Triggers and SQS Queue Mechanisms

### Primary Function Handler
**Function**: `TelegenceGetDevices Lambda FunctionHandler`
**Trigger Method**: SQS Event-driven execution
**Location**: `AltaworxTelegenceAWSGetDevices.cs:48`

```csharp
public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
```

### Trigger Sources

#### 1.1 SQS Queue Triggered Execution
**Primary Trigger**: `TelegenceDestinationQueueGetDevicesURL` SQS Queue
- **Queue Purpose**: Self-continuation and retry mechanism for TelegenceGetDevices Lambda
- **Message Processing**: Each SQS record contains state information for continuation processing
- **Batch Processing**: Processes all records in the SQS event batch

#### 1.2 Manual/Direct Invocation
**Condition**: `sqsEvent?.Records == null` (lines 144-160)
- **Initial State**: Creates default TelegenceGetDevicesSyncState with initial values
- **Default Values**:
  - `CurrentPage = 1`
  - `HasMoreData = true` 
  - `CurrentServiceProviderId = 0`
  - `InitializeProcessing = true`
  - `IsProcessDeviceNotExistsStaging = false`
  - `GroupNumber = 0`
  - `RetryNumber = 0`

### Message Attributes Processing

#### 1.3 SQS Message Attribute Extraction (lines 85-139)
The TelegenceGetDevices Lambda extracts and processes the following message attributes from SQS records:

**CurrentPage** (line 87):
```csharp
syncState.CurrentPage = int.Parse(record.MessageAttributes["CurrentPage"].StringValue);
```
- **Purpose**: Tracks API pagination position
- **Type**: Integer
- **Usage**: Determines which page of Telegence API to request

**HasMoreData** (line 93):
```csharp
syncState.HasMoreData = record.MessageAttributes["HasMoreData"].StringValue.ToLower() == "true";
```
- **Purpose**: Indicates if more API pages exist to process
- **Type**: Boolean (string "true"/"false")
- **Usage**: Controls continuation of pagination loop

**CurrentServiceProviderId** (line 99):
```csharp
syncState.CurrentServiceProviderId = int.Parse(record.MessageAttributes["CurrentServiceProviderId"].StringValue);
```
- **Purpose**: Identifies which service provider is being processed
- **Type**: Integer
- **Usage**: Used for authentication retrieval and data filtering

**InitializeProcessing** (line 106):
```csharp
syncState.InitializeProcessing = record.MessageAttributes["InitializeProcessing"].StringValue.ToLower() == "true";
```
- **Purpose**: Determines if this is initial processing phase (BAN status retrieval)
- **Type**: Boolean (default: true)
- **Usage**: Controls whether to process BAN status or device list

**IsProcessDeviceNotExistsStaging** (line 113):
```csharp
syncState.IsProcessDeviceNotExistsStaging = record.MessageAttributes["IsProcessDeviceNotExistsStaging"].StringValue == "1";
```
- **Purpose**: Indicates device validation phase for existing devices not in staging
- **Type**: Boolean (string "1"/"0")
- **Usage**: Triggers device existence validation processing

**GroupNumber** (line 127):
```csharp
syncState.GroupNumber = int.Parse(record.MessageAttributes["GroupNumber"].StringValue);
```
- **Purpose**: Batch processing group identifier for device validation
- **Type**: Integer
- **Usage**: Processes specific groups of devices for validation

**IsLastProcessDeviceNotExistsStaging** (line 120):
```csharp
syncState.IsLastProcessDeviceNotExistsStaging = record.MessageAttributes["IsLastProcessDeviceNotExistsStaging"].StringValue == "1";
```
- **Purpose**: Indicates final group in device validation processing
- **Type**: Boolean (string "1"/"0")
- **Usage**: Triggers final steps after device validation completion

**RetryNumber** (line 134):
```csharp
syncState.RetryNumber = retryNumber;
```
- **Purpose**: Tracks retry attempts for failed operations
- **Type**: Integer
- **Usage**: Controls retry limits and delay mechanisms

### Queue Interaction Methods

#### 1.4 Self-Requeue Mechanism
**Method**: `SendMessageToGetDevicesQueueAsync` (lines 884-925)
**Purpose**: TelegenceGetDevices Lambda re-queues itself for continuation processing

**Message Attributes Sent**:
```csharp
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

#### 1.5 Downstream Queue Triggers
**TelegenceDeviceUsage Queue Trigger** (lines 927-952):
```csharp
await SendMessageToGetDeviceUsageQueueAsync(context, TelegenceDeviceUsageQueueURL, delaySeconds);
```
- **Triggered**: After successful device processing completion
- **Purpose**: Initiates TelegenceGetDeviceUsage Lambda processing
- **Message Attributes**: `InitializeProcessing = true`

**TelegenceDeviceDetail Queue Trigger** (lines 954-980):
```csharp
await SendMessageToGetDeviceDetailQueueAsync(context, DeviceDetailQueueURL, delaySeconds);
```
- **Triggered**: After device validation completion
- **Purpose**: Initiates TelegenceGetDeviceDetail Lambda processing
- **Message Attributes**: `InitializeProcessing = true`, `GroupNumber = 0`
- **Delay**: 60 seconds (to allow usage processing time)

---

## 2. SQL Retry Logic and Issue Prevention

### Retry Policy Implementation
**Framework**: Polly Resilience Library
**Implementation**: `PolicyFactory.SqlRetryPolicy()` with configurable retry attempts

### 2.1 SQL Retry Configuration
**Retry Attempts**: `CommonConstants.NUMBER_OF_RETRIES` (typically 3)
**Implementation Pattern**:
```csharp
policyFactory.SqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES).Execute(() =>
{
    // SQL operation execution
});
```

### 2.2 Critical Operations Using SQL Retry

#### Device Count Retrieval (lines 280-287)
**Stored Procedure**: `TELEGENCE_GET_CURRENT_DEVICES_COUNT`
**Purpose**: Validates existing device count before processing
**Retry Logic**:
```csharp
policyFactory.SqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES).Execute(() =>
{
    deviceCount = SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(context),
    context.CentralDbConnectionString,
    SQLConstant.StoredProcedureName.TELEGENCE_GET_CURRENT_DEVICES_COUNT,
    parameters,
    SQLConstant.ShortTimeoutSeconds);
});
```

#### BAN List Preparation (lines 334-341)
**Stored Procedure**: `USP_TELEGENCE_DEVICES_PREPARE_BANS_TO_PROCESS`
**Purpose**: Populates BAN processing queue
**Error Handling**: Comprehensive exception logging with retry attempt details

#### BAN List Retrieval (lines 354-357)
**Stored Procedure**: `GET_BAN_LIST_NEED_TO_PROCESS`
**Purpose**: Retrieves BANs requiring status updates
**Return Type**: List<string> of billing account numbers

#### BAN Processing Completion Marking (lines 380-383)
**Stored Procedure**: `MARK_PROCESSED_FOR_BAN`
**Purpose**: Marks BANs as processed after successful API calls
**Parameters**: Comma-separated list of processed BANs

### 2.3 Issues Prevented by SQL Retry Logic

#### Transient Connection Failures
- **Network Hiccups**: Temporary network connectivity issues
- **Connection Pool Exhaustion**: High concurrent Lambda executions
- **DNS Resolution Delays**: Temporary DNS lookup failures

#### Database Lock Contention
- **Deadlock Scenarios**: Multiple Lambda instances accessing same resources
- **Table Lock Timeouts**: Long-running operations blocking access
- **Transaction Rollbacks**: Failed transactions due to resource contention

#### Performance-Related Issues
- **Query Timeouts**: Slow-performing queries due to database load
- **Resource Starvation**: Insufficient database resources during peak load
- **Connection Establishment Delays**: Slow connection creation under load

#### Data Consistency Issues
- **Partial Transaction Failures**: Ensures complete transaction execution
- **Concurrent Modification**: Handles concurrent data updates gracefully
- **State Synchronization**: Maintains consistent processing state

### 2.4 Exception Handling and Logging
**Pattern**: Comprehensive exception capture with detailed logging
```csharp
catch (Exception ex)
{
    logFunction(CommonConstants.EXCEPTION, String.Format(LogCommonStrings.REQUEST_FAILED_AT_FINAL_RETRIES, 
        storedProcedureName, CommonConstants.NUMBER_OF_RETRIES, ex.Message));
}
```

**Logged Information**:
- Stored procedure name
- Number of retry attempts made
- Final exception message
- Stack trace details
- Parameter values (when debugging enabled)

---

## 3. Staging Table Management

### 3.1 Staging Table Clearing Logic
**Trigger Condition**: New service provider processing (`syncState.CurrentServiceProviderId == 0`)
**Location**: Lines 199-201

#### Primary Staging Tables Cleared
**TelegenceDeviceAndUsageStaging Tables** (lines 852-865):
```csharp
private void TruncateTelegenceDeviceAndUsageStaging(KeySysLambdaContext context)
{
    using (var Conn = new SqlConnection(context.CentralDbConnectionString))
    {
        using (var Cmd = new SqlCommand("usp_Telegence_Truncate_DeviceAndUsageStaging", Conn))
        {
            Cmd.CommandType = CommandType.StoredProcedure;
            Cmd.CommandTimeout = 9000; // 150 minutes timeout
            Conn.Open();
            Cmd.ExecuteNonQuery();
        }
    }
}
```

**Tables Cleared by `usp_Telegence_Truncate_DeviceAndUsageStaging`**:
- `TelegenceDeviceStaging`
- `TelegenceDeviceDetailStaging`
- `TelegenceAllUsageStaging`
- `TelegenceDeviceUsageMubuStaging`

**BillingAccountNumberStatus Staging Tables** (lines 868-881):
```csharp
private void TruncateTelegenceBillingAccountNumberStatusStaging(KeySysLambdaContext context)
{
    using (var Conn = new SqlConnection(context.CentralDbConnectionString))
    {
        using (var Cmd = new SqlCommand("usp_Telegence_Truncate_BillingAccountNumberStatusStaging", Conn))
        {
            Cmd.CommandType = CommandType.StoredProcedure;
            Cmd.CommandTimeout = 9000; // 150 minutes timeout
            Conn.Open();
            Cmd.ExecuteNonQuery();
        }
    }
}
```

**Tables Cleared by `usp_Telegence_Truncate_BillingAccountNumberStatusStaging`**:
- `TelegenceDeviceBillingNumberAccountStatusStaging`
- `TelegenceDeviceBANToProcess`

### 3.2 Staging Table Persistence Strategy
**Key Principle**: Staging tables are NOT cleared between retry attempts
**Reason**: Supports continuation processing across Lambda timeouts and retries

#### Benefits of Persistence:
1. **Continuation Processing**: Lambda can resume from previous state
2. **Retry Mechanisms**: Failed operations don't lose previously processed data
3. **Multi-page API Processing**: Maintains state across API pagination
4. **Error Recovery**: Allows recovery from transient failures without data loss

### 3.3 Service Provider Transition Logic
**Trigger**: When `syncState.CurrentServiceProviderId == 0` (initial state)
**Process**:
1. Retrieve next service provider using `ServiceProviderCommon.GetNextServiceProviderId`
2. Clear staging tables for fresh processing
3. Set `syncState.CurrentServiceProviderId` to new provider
4. Begin processing for new service provider

**Error Conditions**:
- **Return 0**: Exception occurred getting service provider
- **Return -1**: No authentication record found
- **Return > 0**: Valid service provider ID to process

### 3.4 Device Not Exists Staging Process
**Table**: `TelegenceDeviceNotExistsStagingToProcess`
**Population**: Via `usp_GetTelegenDevice_NotExists_Stagging` (lines 796-813)

```csharp
protected virtual void GetTelegenceDeviceNotExistsStaging(KeySysLambdaContext context)
{
    using (var con = new SqlConnection(context.CentralDbConnectionString))
    {
        string cmdText = $"usp_GetTelegenDevice_NotExists_Stagging";
        using (var cmd = new SqlCommand(cmdText, con)
        {
            CommandType = CommandType.StoredProcedure
        })
        {
            cmd.CommandTimeout = 800; // 13+ minutes timeout
            con.Open();
            cmd.Parameters.AddWithValue("@BatchSize", BatchSize);
            cmd.ExecuteNonQuery();
        }
    }
}
```

**Process Logic**:
1. **Truncates** `TelegenceDeviceNotExistsStagingToProcess`
2. **Populates** with devices from `TelegenceDevice` that don't exist in `TelegenceDeviceStaging`
3. **Groups** devices using `ROW_NUMBER() OVER(PARTITION BY ServiceProviderId ORDER BY d.id) / @BatchSize`
4. **Filters** devices with `SubscriberNumberStatus <> 'Unknown'`

---

## 4. BAN, FAN, and Number Status Storage

### 4.1 BAN Status Storage Architecture
**Primary Table**: `TelegenceDeviceBillingNumberAccountStatusStaging`
**Purpose**: Caches BAN status information retrieved from Telegence API

#### Table Schema:
- `Id`: Primary identifier
- `BillingAccountNumber`: BAN identifier
- `Status`: BAN status from Telegence API
- `ServiceProviderId`: Associated service provider

### 4.2 BAN Status Retrieval Process
**Method**: `GetBanListStatusesStaging` (lines 391-414)
**SQL Query**:
```sql
SELECT BillingAccountNumber, Status, ServiceProviderId 
FROM TelegenceDeviceBillingNumberAccountStatusStaging 
WHERE BillingAccountNumber IS NOT NULL
```

**Return Type**: `Dictionary<string, string>` (BAN → Status mapping)
**Usage**: Provides cached BAN statuses for device processing without repeated API calls

### 4.3 BAN Status Population Process
**Method**: `SaveBillingAccountNumberStatusStaging` (lines 418-436)
**Process**:
1. **Create DataTable** with schema matching staging table
2. **Populate DataTable** with BAN status dictionary entries
3. **Bulk Copy** to `TelegenceDeviceBillingNumberAccountStatusStaging`

**DataTable Schema**:
```csharp
table.Columns.Add("Id");
table.Columns.Add("BillingAccountNumber");
table.Columns.Add("Status");
table.Columns.Add("ServiceProviderId");
```

### 4.4 Device Storage in TelegenceDeviceStaging
**Method**: `SaveDevicesToStagingTable` (lines 538-572)
**Table**: `TelegenceDeviceStaging`

#### Device Table Schema:
```csharp
table.Columns.Add(CommonColumnNames.Id);                    // Primary Key
table.Columns.Add(CommonColumnNames.ServiceProviderId);     // Service Provider ID
table.Columns.Add(CommonColumnNames.FoundationAccountNumber); // FAN
table.Columns.Add(CommonColumnNames.BillingAccountNumber);   // BAN
table.Columns.Add(CommonColumnNames.SubscriberNumber);       // Phone Number/MSISDN
table.Columns.Add(CommonColumnNames.SubscriberNumberStatus); // Number Status
table.Columns.Add(CommonColumnNames.RefreshTimestamp);       // API Refresh Time
table.Columns.Add(CommonColumnNames.CreatedDate);           // Record Creation Time
table.Columns.Add(CommonColumnNames.BanStatus);             // BAN Status
```

### 4.5 FAN Filtering Logic
**Method**: `GetFANFilter` (called at line 236)
**Purpose**: Applies Foundation Account Number filtering for selective processing

**Filter Types**:
- **IncludedFANs**: Process only devices with specified FANs
- **ExcludedFANs**: Skip devices with specified FANs

**Implementation** (lines 545-552):
```csharp
var includedFANs = fanFilter != null && fanFilter.ContainsKey("IncludedFANs") ? fanFilter["IncludedFANs"] : new List<string>();
var excludedFANs = fanFilter != null && fanFilter.ContainsKey("ExcludedFANs") ? fanFilter["ExcludedFANs"] : new List<string>();

if (includedFANs.Count > 0)
{
    filteredTelegenceDeviceList = filteredTelegenceDeviceList.Where(x => includedFANs.Contains(x.FoundationAccountNumber)).ToList();
}
if (excludedFANs.Count > 0)
{
    filteredTelegenceDeviceList = filteredTelegenceDeviceList.Where(x => !excludedFANs.Contains(x.FoundationAccountNumber)).ToList();
}
```

### 4.6 Data Row Population Logic
**Method**: `AddToDataRow` (lines 827-840)
**Purpose**: Populates DataTable row with device information

**Field Mapping**:
```csharp
dr[1] = currentServiceProviderId;        // ServiceProviderId
dr[2] = device.FoundationAccountNumber;  // FAN
dr[3] = device.BillingAccountNumber;     // BAN
dr[4] = device.SubscriberNumber;         // Number
dr[5] = device.SubscriberNumberStatus;   // Number Status
dr[6] = device.RefreshTimestamp;         // API Timestamp
dr[7] = DateTime.UtcNow;                 // Created Date
dr[8] = banStatusText;                   // BAN Status
```

### 4.7 BAN Status Resolution
**Method**: `GetBanStatusTextForDevice` (lines 816-824)
**Logic**: Resolves BAN status from cached dictionary
```csharp
private string GetBanStatusTextForDevice(Dictionary<string, string> banStatus, TelegenceDeviceResponse telegenceDevice)
{
    string ban = telegenceDevice.BillingAccountNumber;
    if (!string.IsNullOrEmpty(ban) && banStatus.Count > 0 && banStatus.ContainsKey(ban))
    {
        return banStatus[ban];
    }
    return null;
}
```

---

## 5. Telegence API Endpoint Details

### 5.1 API Base Configuration
**Telegence Carrier Base URL**: `https://apsapi.att.com:8082`
**Authentication Method**: Header-based authentication using `app-id` and `app-secret`

### 5.2 Primary API Endpoints

#### Device List Endpoint
**Environment Variable**: `TelegenceDevicesGetURL`
**Purpose**: Retrieves paginated list of devices
**Method**: GET
**Headers**:
- `app-id`: OAuth2ClientId from Integration_Authentication
- `app-secret`: OAuth2ClientSecret from Integration_Authentication  
- `current-page`: Current page number for pagination
- `page-size`: Number of records per page (default: 250)

#### BAN Detail Endpoint
**Environment Variable**: `TelegenceBanDetailGetURL`
**Pattern**: `/billingaccounts/{ban}`
**Purpose**: Retrieves billing account status information
**Method**: GET
**Example**: `/billingaccounts/123456789`

#### Device Detail Endpoint  
**Environment Variable**: `TelegenceDeviceDetailGetURL`
**Pattern**: `/sp/mobility/lineconfig/api/v1/service/{subscriberNumber}`
**Purpose**: Retrieves detailed device configuration
**Method**: GET
**Example**: `/sp/mobility/lineconfig/api/v1/service/2054416784`

### 5.3 Authentication Information Retrieval
**Stored Procedure**: `usp_Telegence_Get_AuthenticationByProviderId`
**Method**: `GetTelegenceAuthenticationInformation` (TelegenceCommon.cs:25-66)

**Retrieved Fields**:
- `integrationAuthenticationId`: Authentication record ID
- `ProductionURL`: Production API base URL
- `SandboxURL`: Sandbox API base URL  
- `Username`: Basic auth username (if applicable)
- `Password`: Basic auth password (if applicable)
- `WriteIsEnabled`: Write operation permission flag
- `BillPeriodEndDay`: Billing cycle end day
- `OAuth2ClientId`: Client ID for header authentication
- `OAuth2ClientSecret`: Client secret for header authentication

### 5.4 API Call Implementation Patterns

#### Proxy-Based API Calls (TelegenceCommon.cs:496-518)
**Used When**: `proxyUrl` is configured
**Method**: `GetTelegenceDeviceBySubscriberNumberByProxy`
**Payload Structure**:
```csharp
var payload = new PayloadModel()
{
    AuthenticationType = AuthenticationType.TELEGENCEAUTH,
    Endpoint = deviceDetailUrl,
    HeaderContent = headerContentString,
    JsonContent = null,
    Password = null,
    Token = null,
    Url = baseUrl,
    Username = null
};
```

#### Direct API Calls (TelegenceCommon.cs:521-540)
**Used When**: No proxy configuration
**Method**: `GetTelegenceDeviceBySubscriberNumberWithoutProxy`
**Headers Applied**:
```csharp
client.DefaultRequestHeaders.Add(CommonConstants.APP_ID, telegenceAuth.ClientId);
client.DefaultRequestHeaders.Add(CommonConstants.APP_SECRET, telegenceAuth.ClientSecret);
```

### 5.5 Environment vs Production URL Selection
**Logic** (TelegenceCommon.cs:365-368):
```csharp
Uri baseUrl = new Uri(telegenceAuth.SandboxUrl);
if (isProduction)
{
    baseUrl = new Uri(telegenceAuth.ProductionUrl);
}
```

**Determination**: Based on `context.IsProduction` flag from Lambda context

---

## 6. API Pagination Configuration and Completion Detection

### 6.1 Pagination Configuration
**Page Size**: Configurable via `BatchSize` environment variable (default: 250)
**Environment Variable**: `BatchSize`
**Default Value**: `DEFAULT_BATCH_SIZE = 250`
**Configuration Logic** (lines 66-70):
```csharp
if (context.ClientContext.Environment["BatchSize"].Length > 0)
{
    var batchSize = int.Parse(context.ClientContext.Environment["BatchSize"]);
    BatchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
}
```

### 6.2 Pagination State Management
**State Object**: `TelegenceGetDevicesSyncState`
**Key Properties**:
- `CurrentPage`: Current API page being processed
- `HasMoreData`: Boolean indicating more pages available
- `IsLastCycle`: Boolean indicating final processing cycle

### 6.3 Pagination Headers in API Requests

#### Proxy-Based Requests (TelegenceCommon.cs:547-548)
```csharp
headerContent.Add(CommonConstants.CURRENT_PAGE, syncState.CurrentPage.ToString());
headerContent.Add(CommonConstants.PAGE_SIZE, pageSize);
```

#### Direct API Requests (TelegenceCommon.cs:598-599)
```csharp
client.DefaultRequestHeaders.Add(CommonConstants.CURRENT_PAGE, syncState.CurrentPage.ToString());
client.DefaultRequestHeaders.Add(CommonConstants.PAGE_SIZE, pageSize.ToString());
```

### 6.4 Pagination Completion Detection

#### Method 1: Page Total Header Analysis (TelegenceCommon.cs:567-570)
**Proxy Response**:
```csharp
if (int.TryParse(headers[CommonConstants.PAGE_TOTAL].ToString(), out int pageTotal))
{
    syncState.HasMoreData = syncState.CurrentPage < pageTotal;
}
syncState.IsLastCycle = !syncState.HasMoreData;
```

**Direct Response** (TelegenceCommon.cs:607-611):
```csharp
if (int.TryParse(responseMessage.Headers.GetValues(CommonConstants.PAGE_TOTAL).FirstOrDefault(), out int pageTotal))
{
    syncState.HasMoreData = syncState.CurrentPage < pageTotal;
}
syncState.IsLastCycle = !syncState.HasMoreData;
```

#### Method 2: Empty Response Detection
**Logic**: If API returns empty device list, assume completion
**Implementation**: Implicit in device list processing

### 6.5 Pagination Control Mechanisms

#### Max Cycles Per Lambda Execution
**Configuration**: `MaxCyclesToProcess` environment variable
**Purpose**: Limits API calls per Lambda execution to prevent timeouts
**Implementation** (lines 455-476):
```csharp
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

#### Timeout-Based Pagination Control
**Timeout Threshold**: `CommonConstants.REMAINING_TIME_CUT_OFF` (typically 180 seconds)
**Behavior**: Stops pagination when Lambda execution time is insufficient
**Recovery**: Increments retry counter and re-queues for continuation

### 6.6 Continuation Processing Logic
**Trigger Condition** (lines 501-504):
```csharp
if ((!syncState.IsLastCycle || syncState.HasMoreData) && syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES)
{
    await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, delaySeconds: CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS);
}
```

**State Preservation**: All pagination state preserved in SQS message attributes for seamless continuation

---

## 7. Device Validation and Failure Handling

### 7.1 Device Status Validation Logic
**Primary Validation**: Subscriber number status change detection
**Location**: `ProcessDeviceNotExistsStagingAsync` method (lines 596-696)

#### Status Change Detection (lines 637-650)
```csharp
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

### 7.2 Validation Rules

#### Rule 1: Status Change Requirement
**Logic**: Only process devices where current status differs from API status
**Implementation**: `telegenceDevice.SubscriberNumberStatus != subscriberStatus`
**Purpose**: Avoid processing devices with unchanged status

#### Rule 2: Cancelled Status Exclusion  
**Logic**: Ignore devices with cancelled status
**Implementation**: `subscriberStatus != CANCEL_STATUS` where `CANCEL_STATUS = "C"`
**Reason**: Device List API doesn't return cancelled devices, so they shouldn't be processed

#### Rule 3: Unknown Status Filtering
**Location**: `usp_GetTelegenDevice_NotExists_Stagging` stored procedure
**Logic**: `SubscriberNumberStatus <> 'Unknown'`
**Purpose**: Skip devices with unknown status from validation

### 7.3 Device Detail API Validation Process

#### Individual Device API Call (lines 622-628)
```csharp
string resultAPI = await TelegenceCommon.GetTelegenceDeviceBySubscriberNumber(context, telegenceAuthenticationInfo, context.IsProduction,
                    telegenceDevice.SubscriberNumber, TelegenceDeviceDetailGetURL, ProxyUrl);
```

#### Response Processing (lines 630-651)
```csharp
if (!string.IsNullOrWhiteSpace(resultAPI))
{
    var deviceDetail = JsonConvert.DeserializeObject<TelegenceMobilityLineConfigurationResponse>(resultAPI);
    var subscriberStatus = deviceDetail.serviceCharacteristic.Where(x => x.Name == SUBSCRIBER_STATUS).Select(x => x.Value).FirstOrDefault();
    if (!string.IsNullOrEmpty(subscriberStatus))
    {
        // Validation and processing logic
    }
}
```

### 7.4 Failure Handling Mechanisms

#### API Call Failure Handling
**Empty Response**: When `string.IsNullOrWhiteSpace(resultAPI)` is true
**Action**: Device is skipped but marked as processed
**Logging**: No explicit error logging for empty responses (treated as normal)

#### JSON Deserialization Failure
**Exception Handling**: Implicit through null checking
**Recovery**: Skip device processing, continue with next device
**State Preservation**: Device marked as processed to avoid reprocessing

#### Authentication Failure (lines 623-626)
```csharp
if (telegenceAuthenticationInfo == null)
{
    throw new Exception($"Error Getting Info: Failed to get Telegence Authentication Information.");
}
```
**Action**: Throws exception, stops batch processing
**Recovery**: Lambda retry mechanism handles authentication issues

### 7.5 Timeout-Based Processing Control
**Timeout Check** (line 619):
```csharp
if (context.Context.RemainingTime.TotalSeconds > CommonConstants.REMAINING_TIME_CUT_OFF)
```

**Timeout Action** (lines 657-661):
```csharp
else
{
    LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.STOP_GETTING_SINCE_IS_NOT_ENOUGH_TIME, "Telegence Device Details"));
    syncState.RetryNumber++;
    break;
}
```

### 7.6 Processed Device Tracking
**Method**: `MarkProcessForEachDevicesHaveProcessed` (lines 720-736)
**Stored Procedure**: `MARK_TELEGENCE_DEVICES_PROCESSED_ON_PROCESS_CHECK_EXISTS_ON_STAGING`

**Parameters**:
```csharp
var parameters = new List<SqlParameter>()
{
    new SqlParameter(CommonSQLParameterNames.SUBSCRIBER_NUMBERS, string.Join(",", subscriberNumbers)),
};
```

**Purpose**: Prevents reprocessing of already validated devices

### 7.7 Batch Processing Completion Logic
**Successful Processing** (lines 663-667):
```csharp
if (table.Rows.Count > 0)
{
    LogInfo(context, "STATUS", "Insert SIMs exists in AMOP not exists in Device List From Api.");
    SqlBulkCopy(context, context.CentralDbConnectionString, table, "TelegenceDeviceStaging");
}
```

**Continuation Logic** (lines 676-684):
```csharp
if (remainingDevicesNeedToProcesses.Count > 0 && syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES)
{
    // Continue processing remaining devices
    await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS, syncState.GroupNumber, 1, isLastProcessDeviceNotExistsStaging);
}
```

**Final Processing** (lines 689-695):
```csharp
if (syncState.IsLastProcessDeviceNotExistsStaging)
{
    LogInfo(context, "INFO", $"END ProcessDeviceNotExistsStagingAsync --> Run Process Get Usage And Get Detail.");
    
    await SendMessageToGetDeviceUsageQueueAsync(context, TelegenceDeviceUsageQueueURL, CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS);
    await SendMessageToGetDeviceDetailQueueAsync(context, DeviceDetailQueueURL, delayQueue); // give usage time to process before starting detail processing
}
```

---

## 8. Retry Configuration using Polly

### 8.1 Polly Retry Framework Integration
**Library**: Polly Resilience and Transient-Fault-Handling Library
**Implementation**: `RetryPolicyHelper.PollyRetryForProxyRequestAsync` and `PollyRetryHttpRequestAsync`

### 8.2 Telegence API Retry Configuration
**Retry Attempts**: `CommonConstants.NUMBER_OF_TELEGENCE_RETRIES` (typically 3)
**Retry Scope**: All Telegence API calls (device list, device detail, BAN status)

#### Proxy-Based API Retry (TelegenceCommon.cs:502-510)
```csharp
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

#### Direct HTTP API Retry (TelegenceCommon.cs:524-532)
```csharp
var responseMessage = await Amop.Core.Helpers.RetryPolicyHelper.PollyRetryHttpRequestAsync(context.logger, CommonConstants.NUMBER_OF_TELEGENCE_RETRIES).ExecuteAsync(async () =>
{
    using (var client = new HttpClient())
    {
        ConfigHttpClient(client);
        BuildRequestHeaders(client, telegenceAuthentication);
        return await client.GetAsync(deviceDetailUrl);
    }
});
```

### 8.3 HTTP Client Configuration
**Method**: `ConfigHttpClient` (TelegenceCommon.cs:702-704)
**Timeout**: `CommonConstants.HTTP_CLIENT_REQUEST_TIMEOUT_IN_MINUTES` minutes
```csharp
private static void ConfigHttpClient(HttpClient httpClient)
{
    httpClient.Timeout = TimeSpan.FromMinutes(CommonConstants.HTTP_CLIENT_REQUEST_TIMEOUT_IN_MINUTES);
}
```

### 8.4 Lambda-Level Retry Configuration
**Retry Limit**: `CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES` (typically 5)
**Application**: TelegenceGetDevices Lambda self-retry mechanism

#### BAN Processing Retry (lines 221-224)
```csharp
if (remainingBanNeedToProcesses.Count > 0 && syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES)
{
    await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, delaySeconds: CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS);
}
```

#### Device List Processing Retry (lines 501-504)
```csharp
if ((!syncState.IsLastCycle || syncState.HasMoreData) && syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES)
{
    await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, delaySeconds: CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS);
}
```

#### Device Validation Retry (lines 676-683)
```csharp
if (remainingDevicesNeedToProcesses.Count > 0 && syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES)
{
    await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS, syncState.GroupNumber, 1, isLastProcessDeviceNotExistsStaging);
}
```

### 8.5 Retry Delay Configuration
**Delay Duration**: `CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS` (5 seconds)
**Purpose**: Prevents immediate retry and reduces API load
**Implementation**: SQS message delay mechanism

### 8.6 Retry State Management
**State Tracking**: `syncState.RetryNumber` property
**Increment Logic**: Incremented on timeout or failure conditions
**Reset Logic**: Reset to 0 on successful phase transitions

#### Timeout-Based Retry Increment (lines 472-473)
```csharp
syncState.RetryNumber++;
LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.STOP_GETTING_SINCE_IS_NOT_ENOUGH_TIME, "Telegence Devices"));
```

#### BAN Processing Retry Increment (lines 319-320)
```csharp
LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.STOP_GETTING_SINCE_IS_NOT_ENOUGH_TIME, "BAN"));
syncState.RetryNumber++;
```

#### Device Validation Retry Increment (lines 659-660)
```csharp
LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.STOP_GETTING_SINCE_IS_NOT_ENOUGH_TIME, "Telegence Device Details"));
syncState.RetryNumber++;
```

### 8.7 Retry Failure Conditions

#### API-Level Failures (Handled by Polly)
- **HTTP Timeouts**: Request exceeds configured timeout
- **Connection Failures**: Network connectivity issues
- **HTTP 5xx Errors**: Server-side errors
- **DNS Resolution Failures**: Temporary DNS issues

#### Lambda-Level Failures (Handled by Lambda Retry)
- **Lambda Timeouts**: Execution time limit approached
- **Processing Incomplete**: More data to process than single execution can handle
- **Resource Exhaustion**: Insufficient Lambda resources
- **Database Connection Issues**: Transient database connectivity problems

### 8.8 Exponential Backoff Strategy
**Implementation**: Built into Polly retry policies
**Benefits**:
- **Reduces API Load**: Prevents overwhelming external services
- **Improves Success Rate**: Allows transient issues to resolve
- **Distributes Load**: Staggers retry attempts across time

---

## 9. Re-enqueuing for Incomplete Processing

### 9.1 Re-enqueuing Trigger Conditions
The TelegenceGetDevices Lambda re-enqueues itself for continuation processing under multiple scenarios:

#### Condition 1: Pagination Continuation (lines 501-504)
```csharp
if ((!syncState.IsLastCycle || syncState.HasMoreData) && syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES)
{
    await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, delaySeconds: CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS);
}
```
**Triggers**:
- More API pages available (`!syncState.IsLastCycle`)
- Explicit more data flag (`syncState.HasMoreData`)
- Within retry limits (`syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES`)

#### Condition 2: BAN Processing Continuation (lines 221-224)
```csharp
if (remainingBanNeedToProcesses.Count > 0 && syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES)
{
    await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, delaySeconds: CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS);
}
```
**Triggers**:
- Remaining BANs need processing (`remainingBanNeedToProcesses.Count > 0`)
- Within retry limits

#### Condition 3: Device Validation Continuation (lines 676-683)
```csharp
if (remainingDevicesNeedToProcesses.Count > 0 && syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES)
{
    await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS, syncState.GroupNumber, 1, isLastProcessDeviceNotExistsStaging);
}
```
**Triggers**:
- Remaining devices need validation (`remainingDevicesNeedToProcesses.Count > 0`)
- Within retry limits

#### Condition 4: Timeout-Based Continuation
**Timeout Threshold**: `CommonConstants.REMAINING_TIME_CUT_OFF` (180 seconds)
**Logic**: When Lambda execution time is insufficient to complete current operation
**Action**: Increment retry counter and re-enqueue for continuation

### 9.2 State Preservation Mechanism
**Method**: `SendMessageToGetDevicesQueueAsync` (lines 884-925)
**Purpose**: Preserves complete processing state in SQS message attributes

#### Complete State Preservation (lines 905-914)
```csharp
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

### 9.3 Phase-Specific Re-enqueuing

#### BAN Processing Phase Re-enqueuing
**Phase**: `InitializeProcessing = true`
**State Changes**:
- Maintains current service provider ID
- Increments retry number on timeout
- Resets retry number on successful phase completion

#### Device List Processing Phase Re-enqueuing  
**Phase**: `InitializeProcessing = false`
**State Changes**:
- Increments current page number
- Updates HasMoreData based on API response
- Maintains pagination state across Lambda executions

#### Device Validation Phase Re-enqueuing
**Phase**: `IsProcessDeviceNotExistsStaging = true`
**State Changes**:
- Maintains group number for batch processing
- Tracks last group processing flag
- Preserves device validation state

### 9.4 Group-Based Re-enqueuing for Device Validation
**Method**: `SendProcessDeviceNotExistsStagingMessagesToQueueAsync` (lines 751-766)
**Purpose**: Creates multiple SQS messages for parallel device validation processing

```csharp
for (int iGroup = 0; iGroup <= groupCount; iGroup++)
{
    if (iGroup == groupCount)
    {
        delayQueue = 60;  // Last group gets 60-second delay
        isLastGroup = 1;
    }
    await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, delayQueue, iGroup, 1, isLastGroup);
}
```

**Benefits**:
- **Parallel Processing**: Multiple Lambda instances process different groups
- **Load Distribution**: Spreads processing across time and resources
- **Fault Isolation**: Group failures don't affect other groups

### 9.5 Delay Strategy for Re-enqueuing
**Standard Delay**: `CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS` (5 seconds)
**Extended Delay**: 60 seconds for final group processing
**Purpose**:
- **Rate Limiting**: Prevents overwhelming downstream systems
- **Resource Management**: Allows Lambda resources to be released
- **Coordination**: Ensures proper sequencing of dependent operations

### 9.6 Re-enqueuing Failure Handling
**SQS Send Failure**: If SQS message sending fails, Lambda execution completes without continuation
**Retry Limit Exceeded**: Processing stops when `syncState.RetryNumber > CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES`
**Dead Letter Queue**: Failed messages may be sent to DLQ for manual investigation (configured at SQS level)

### 9.7 Message Body Content
**Standard Message Body**: `"Continuing processing of Telegence devices"`
**Device Usage Message Body**: `"Start processing Telegence device usage"`  
**Device Detail Message Body**: `"Start processing Telegence device details"`
**Purpose**: Provides human-readable context for monitoring and debugging

---

## 10. Stored Procedures in the Flow

### 10.1 Service Provider Management Procedures

#### `usp_DeviceSync_Get_NextServiceProviderIdByIntegration`
**Purpose**: Retrieves next service provider for Telegence integration processing
**Parameters**: 
- `@providerId` (int): Current service provider ID
- `@integrationId` (int): Integration type ID (6 for Telegence)
**Returns**: Next service provider ID or -1 if none available
**Usage**: Called via `ServiceProviderCommon.GetNextServiceProviderId`

**SQL Logic**:
```sql
SELECT TOP (1) srvP.id as nxtSrvPId  
FROM dbo.ServiceProvider srvP   
INNER JOIN [dbo].[Integration_Authentication] intAuth on srvP.id = intAuth.ServiceProviderId  
WHERE srvP.IntegrationId = @integrationid  
  AND srvP.IsActive = 1  
  AND srvP.IsDeleted = 0  
  AND intAuth.isDeleted = 0  
  AND intAuth.isActive = 1  
  AND srvP.id > @providerId  
ORDER BY srvP.id
```

#### `usp_Telegence_Get_AuthenticationByProviderId`
**Purpose**: Retrieves authentication information for specific service provider
**Parameters**: `@providerId` (int)
**Returns**: Authentication details including ClientId, ClientSecret, URLs
**Usage**: Called by `TelegenceCommon.GetTelegenceAuthenticationInformation`

**Key Fields Returned**:
- `integrationAuthenticationId`: Authentication record ID
- `ProductionURL`, `SandboxURL`: API base URLs
- `username`, `password`: Basic authentication credentials
- `WriteIsEnabled`: Write operation permission
- `BillPeriodEndDay`: Billing cycle configuration
- `OAuth2ClientId`, `OAuth2ClientSecret`: Header authentication credentials

### 10.2 Device Count and Validation Procedures

#### `TELEGENCE_GET_CURRENT_DEVICES_COUNT`
**Purpose**: Counts existing devices for specific service provider
**Parameters**: `@ServiceProviderId` (int)
**Returns**: Integer count of existing devices
**Usage**: Determines if service provider is in first sync mode
**Location**: Called in `GetTelegenceCurrentDeviceCount` (lines 282-286)

#### `usp_GetTelegenDevice_NotExists_Stagging`
**Purpose**: Populates staging table with devices needing validation
**Parameters**: `@BatchSize` (int)
**Process**:
1. Truncates `TelegenceDeviceNotExistsStagingToProcess`
2. Inserts devices from `TelegenceDevice` not in `TelegenceDeviceStaging`
3. Assigns group numbers for batch processing
4. Filters out devices with 'Unknown' status

**SQL Logic**:
```sql
INSERT INTO [dbo].[TelegenceDeviceNotExistsStagingToProcess]([SubscriberNumber], [ServiceProviderId], [FoundationAccountNumber], [BillingAccountNumber], [SubscriberNumberStatus], [GroupNumber])  
SELECT SubscriberNumber, ServiceProviderId, FoundationAccountNumber, BillingAccountNumber, SubscriberNumberStatus,
       ROW_NUMBER() OVER(PARTITION BY ServiceProviderId ORDER BY d.id) / @BatchSize AS GroupNumber  
FROM TelegenceDevice d  
WHERE NOT EXISTS (SELECT 1 FROM TelegenceDeviceStaging s WHERE s.subscriberNumber = d.subscriberNumber)  
  AND SubscriberNumberStatus <> 'Unknown'
```

#### `GET_TELEGENCE_DEVICES_NOT_EXISTS_ON_STAGING`
**Purpose**: Retrieves devices from staging table for validation processing
**Parameters**: `@GroupNumber` (int)
**Returns**: List of devices needing API validation
**Usage**: Called by `GetTelegenceDeviceNotExistsOnStagingToProcess`

**Conditional Logic**:
- If `@groupNumber < 0`: Returns all unprocessed devices
- If `@groupNumber >= 0`: Returns devices for specific group

### 10.3 BAN Processing Procedures

#### `usp_Telegence_Devices_Prepare_BANs_To_Process`
**Purpose**: Prepares BAN list for status processing
**Parameters**: None
**Process**: Inserts distinct BANs from TelegenceDevice into TelegenceDeviceBANToProcess
**SQL Logic**:
```sql
INSERT INTO [dbo].[TelegenceDeviceBANToProcess] ([BillingAccountNumber])  
SELECT DISTINCT [BillingAccountNumber]  
FROM [TelegenceDevice]  
WHERE ISNULL([BillingAccountNumber], '') <> ''
```

#### `usp_Get_BAN_List_Need_To_Process`
**Purpose**: Retrieves BANs needing status updates
**Parameters**: None
**Returns**: List of unprocessed billing account numbers
**SQL Logic**:
```sql
SELECT [BillingAccountNumber]  
FROM [TelegenceDeviceBANToProcess]  
WHERE [IsProcessed] = 0
```

#### `usp_Mark_Processed_For_BAN`
**Purpose**: Marks BANs as processed after successful API calls
**Parameters**: `@billingAccountNumbers` (NVARCHAR(MAX)) - Comma-separated BAN list
**Process**: 
1. Creates temporary table with BANs
2. Updates TelegenceDeviceBANToProcess.IsProcessed = 1
3. Uses ROWLOCK for concurrency control

### 10.4 Device Processing Completion Procedures

#### `MARK_TELEGENCE_DEVICES_PROCESSED_ON_PROCESS_CHECK_EXISTS_ON_STAGING`
**Purpose**: Marks devices as processed after validation
**Parameters**: `@SubscriberNumbers` (comma-separated list)
**Usage**: Prevents reprocessing of validated devices
**Location**: Called by `MarkProcessForEachDevicesHaveProcessed`

### 10.5 Staging Table Management Procedures

#### `usp_Telegence_Truncate_DeviceAndUsageStaging`
**Purpose**: Clears device and usage staging tables
**Tables Cleared**:
- `TelegenceDeviceStaging`
- `TelegenceDeviceDetailStaging`  
- `TelegenceAllUsageStaging`
- `TelegenceDeviceUsageMubuStaging`
**Usage**: Called at start of new service provider processing

#### `usp_Telegence_Truncate_BillingAccountNumberStatusStaging`
**Purpose**: Clears BAN status staging tables
**Tables Cleared**:
- `TelegenceDeviceBillingNumberAccountStatusStaging`
- `TelegenceDeviceBANToProcess`
**Usage**: Called at start of new service provider processing

### 10.6 Procedure Execution Patterns

#### SQL Retry Pattern
**Implementation**: All procedures wrapped in `policyFactory.SqlRetryPolicy().Execute()`
**Retry Count**: `CommonConstants.NUMBER_OF_RETRIES` (typically 3)
**Error Handling**: Comprehensive exception logging with procedure name and retry count

#### Timeout Configuration
**Short Operations**: `SQLConstant.ShortTimeoutSeconds`
**Long Operations**: Custom timeouts (800 seconds for staging population)
**Staging Operations**: 9000 seconds (150 minutes) for truncate operations

#### Parameter Handling
**Parameterized Queries**: All procedures use SqlParameter objects
**SQL Injection Prevention**: Parameters prevent SQL injection attacks
**Type Safety**: Strongly typed parameter values

---

## 11. Summary Logging Details

### 11.1 Logging Framework Integration
**Base Class**: `AwsFunctionBase.LogInfo` method
**Log Categories**: STATUS, INFO, WARNING, EXCEPTION, SUB (sub-process)
**Context**: All logging includes Lambda context for correlation

### 11.2 Processing Phase Logging

#### Lambda Initialization Logging (lines 77-82)
```csharp
LogInfo(keysysContext, "STATUS", $"TelegenceGetDevices::Beginning to process {processedRecordCount} records...");
foreach (var record in sqsEvent.Records)
{
    LogInfo(keysysContext, "MessageId", record.MessageId);
    LogInfo(keysysContext, "EventSource", record.EventSource);
    LogInfo(keysysContext, "Body", record.Body);
}
```

#### SQS Message Attribute Logging (lines 88-138)
**Logged Attributes**:
- `CurrentPage`: API pagination position
- `HasMoreData`: More data availability flag
- `CurrentServiceProviderId`: Service provider being processed
- `InitializeProcessing`: Processing phase indicator
- `IsProcessDeviceNotExistsStaging`: Device validation phase flag
- `GroupNumber`: Batch processing group
- `RetryNumber`: Retry attempt counter

#### Service Provider Processing Logging (lines 191-202)
```csharp
LogInfo(context, "WARNING", $"Error Getting a service provider id for Telegence CurrentServiceProviderId: {syncState.CurrentServiceProviderId}");
LogInfo(context, "WARNING", "No Authentication record was found for Telegence Service Provider.");
```

### 11.3 API Interaction Logging

#### BAN Processing Logging (lines 217-218)
```csharp
LogInfo(context, "SUB: TryProcessDeviceListAsync", "Get Telegence Billing Account Number Status");
```

#### Device API Call Logging (TelegenceCommon.cs:361-363)
```csharp
AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.GET_DEVICE_DETAIL_BY_SUBCRIBER_NUMBER, subscriberNo));
AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, $"endpoint: {deviceDetailEndpoint}");
```

#### API Success/Failure Logging (TelegenceCommon.cs:514-517)
```csharp
AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.REQUEST_URL_SUCCESS, deviceDetailUrl));
// On failure:
AwsFunctionBase.LogInfo(context, LogTypeConstant.Error, string.Format(LogCommonStrings.REQUEST_FAILED_RESPONSE_AT_FINAL_RETRIES, deviceDetailUrl, responseBody));
```

### 11.4 Database Operation Logging

#### SQL Bulk Copy Logging (line 571)
```csharp
LogInfo(context, CommonConstants.STATUS, LogCommonStrings.SQL_BULK_COPY_START);
```

#### Stored Procedure Execution Logging (SqlQueryHelper.cs)
**Parameter Logging**:
```csharp
logFunction(CommonConstants.INFO, string.Format(LogCommonStrings.EXECUTING_STORED_PROCEDURE_WITH_PARAMETERS, storedProcedureName) + 
    string.Join(Environment.NewLine, parameters.Select(parameter => $"{parameter.ParameterName}: {parameter.Value}")));
```

**Result Logging**:
```csharp
logFunction(CommonConstants.INFO, string.Format(LogCommonStrings.ROWS_AFFECTED_WHEN_EXECUTING_STORED_PROCEDURE, affectedRows, storedProcedureName));
```

### 11.5 Timeout and Retry Logging

#### Timeout Detection Logging (lines 318-319, 473-474, 659-660)
```csharp
LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.STOP_GETTING_SINCE_IS_NOT_ENOUGH_TIME, "BAN"));
LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.STOP_GETTING_SINCE_IS_NOT_ENOUGH_TIME, "Telegence Devices"));  
LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.STOP_GETTING_SINCE_IS_NOT_ENOUGH_TIME, "Telegence Device Details"));
```

#### Retry Attempt Logging
**SQL Retry Failures**:
```csharp
logFunction(CommonConstants.EXCEPTION, String.Format(LogCommonStrings.REQUEST_FAILED_AT_FINAL_RETRIES, 
    storedProcedureName, CommonConstants.NUMBER_OF_RETRIES, ex.Message));
```

### 11.6 Processing Completion Logging

#### Device Validation Logging (lines 614, 665, 687)
```csharp
LogInfo(context, "Device Need Check API", $"Has {telegenceDevicesFromProcess.Count} Need Check API.");
LogInfo(context, "STATUS", "Insert SIMs exists in AMOP not exists in Device List From Api.");
LogInfo(context, "INFO", $"END ProcessDeviceNotExistsStagingAsync: has {table.Rows.Count} added TelegenceDeviceStaging.");
```

#### Final Processing Logging (lines 691-692)
```csharp
LogInfo(context, "INFO", $"END ProcessDeviceNotExistsStagingAsync --> Run Process Get Usage And Get Detail.");
```

#### Queue Message Sending Logging (lines 887-924)
```csharp
LogInfo(context, "SUB", "SendMessageToGetDevicesQueueAsync");
LogInfo(context, "HasMoreData", syncState.HasMoreData);
LogInfo(context, "CurrentPage", syncState.CurrentPage);
LogInfo(context, "CurrentServiceProviderId", syncState.CurrentServiceProviderId);
LogInfo(context, "TelegenceDestinationQueueGetDevicesURL", telegenceDestinationQueueGetDevicesURL);
LogInfo(context, "InitializeProcessing", syncState.InitializeProcessing);
LogInfo(context, "DelaySeconds", delaySeconds);
LogInfo(context, "groupNumber", groupNumber);
LogInfo(context, "isProcessDeviceNotExistsStaging", isProcessDeviceNotExistsStaging);
LogInfo(context, "isLastGroup", isLastGroup);
LogInfo(context, "RetryNumber", syncState.RetryNumber);
LogInfo(context, "MessageBody", request.MessageBody);
LogInfo(context, "RESPONSE STATUS", $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
```

### 11.7 Exception Logging

#### Comprehensive Exception Handling (lines 164-173)
```csharp
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

#### Specific Process Exception Logging (lines 250-253)
```csharp
catch (Exception ex)
{
    LogInfo(context, "EXCEPTION:TelegenceGetDevices:", ex.Message + " " + ex.StackTrace);
}
```

### 11.8 Performance and Metrics Logging

#### Processing Counts and Timing
- Record processing counts at start and completion
- API call counts and response times
- Database operation execution times
- Batch size and pagination metrics

#### Resource Usage Logging
- Lambda remaining time monitoring
- Memory usage implications (via Lambda context)
- Database connection and timeout tracking

---

## 12. System Orchestration

### 12.1 Lambda Function Ecosystem
The TelegenceGetDevices Lambda operates as part of a coordinated ecosystem of AWS Lambda functions for Telegence data synchronization.

#### Primary Functions in Ecosystem
1. **TelegenceGetDevices Lambda**: Main device list synchronization (this document)
2. **TelegenceGetDeviceUsage Lambda**: Device usage data synchronization
3. **TelegenceGetDeviceDetail Lambda**: Detailed device configuration synchronization

### 12.2 Queue-Based Orchestration Architecture

#### SQS Queue Relationships
**TelegenceDestinationQueueGetDevicesURL**:
- **Producer**: TelegenceGetDevices Lambda (self-enqueuing)
- **Consumer**: TelegenceGetDevices Lambda
- **Purpose**: Continuation processing and retry mechanism

**TelegenceDeviceUsageQueueURL**:
- **Producer**: TelegenceGetDevices Lambda
- **Consumer**: TelegenceGetDeviceUsage Lambda  
- **Trigger Condition**: After successful device processing completion

**TelegenceDeviceDetailQueueURL**:
- **Producer**: TelegenceGetDevices Lambda
- **Consumer**: TelegenceGetDeviceDetail Lambda
- **Trigger Condition**: After device validation completion

### 12.3 Processing Flow Orchestration

#### Phase 1: Service Provider Initialization
**Trigger**: Manual invocation or scheduled event
**Process**:
1. Retrieve next service provider via `usp_DeviceSync_Get_NextServiceProviderIdByIntegration`
2. Clear staging tables via truncate procedures
3. Initialize processing state with `CurrentServiceProviderId`

#### Phase 2: BAN Status Processing (`InitializeProcessing = true`)
**Process Flow**:
1. **Prepare BAN List**: Execute `usp_Telegence_Devices_Prepare_BANs_To_Process`
2. **Retrieve BAN List**: Execute `usp_Get_BAN_List_Need_To_Process`
3. **API Calls**: Call Telegence BAN status API for each BAN
4. **Mark Processed**: Execute `usp_Mark_Processed_For_BAN`
5. **State Transition**: Set `InitializeProcessing = false`
6. **Continuation**: Re-enqueue for device list processing

#### Phase 3: Device List Processing (`InitializeProcessing = false`)
**Process Flow**:
1. **Retrieve BAN Status**: Load cached statuses from staging
2. **API Pagination**: Call Telegence device list API with pagination
3. **Device Filtering**: Apply FAN include/exclude filters
4. **Bulk Insert**: Save devices to `TelegenceDeviceStaging`
5. **Pagination Control**: Continue until `IsLastCycle = true`
6. **Service Provider Check**: Check for next service provider
7. **Phase Transition**: Initiate device validation phase

#### Phase 4: Device Validation Processing (`IsProcessDeviceNotExistsStaging = true`)
**Process Flow**:
1. **Populate Validation Queue**: Execute `usp_GetTelegenDevice_NotExists_Stagging`
2. **Group Processing**: Create SQS messages for each device group
3. **Parallel Validation**: Multiple Lambda instances validate device groups
4. **API Detail Calls**: Individual device detail API calls for validation
5. **Status Comparison**: Compare existing vs API device status
6. **Update Staging**: Insert changed devices to staging
7. **Mark Processed**: Execute device processed marking procedures
8. **Final Triggers**: Trigger usage and detail processing Lambdas

### 12.4 Downstream Lambda Triggers

#### TelegenceGetDeviceUsage Lambda Trigger
**Method**: `SendMessageToGetDeviceUsageQueueAsync` (lines 927-952)
**Trigger Timing**: After device processing completion
**Message Attributes**:
- `InitializeProcessing = true`: Indicates fresh start for usage processing
**Delay**: `CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS` (5 seconds)

#### TelegenceGetDeviceDetail Lambda Trigger  
**Method**: `SendMessageToGetDeviceDetailQueueAsync` (lines 954-980)
**Trigger Timing**: After device validation completion
**Message Attributes**:
- `InitializeProcessing = true`: Indicates fresh start for detail processing
- `GroupNumber = 0`: Reset group processing
**Delay**: 60 seconds (allows usage processing to complete first)

### 12.5 Error Handling and Recovery Orchestration

#### Lambda-Level Error Recovery
**Retry Mechanism**: Self-enqueuing with state preservation
**Retry Limit**: `CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES`
**State Preservation**: Complete state maintained in SQS message attributes
**Failure Mode**: Processing stops after retry limit exceeded

#### API-Level Error Recovery
**Retry Framework**: Polly resilience library
**Retry Count**: `CommonConstants.NUMBER_OF_TELEGENCE_RETRIES`
**Scope**: All Telegence API calls
**Backoff Strategy**: Exponential backoff with jitter

#### Database-Level Error Recovery
**Retry Framework**: Polly SQL retry policies
**Retry Count**: `CommonConstants.NUMBER_OF_RETRIES`
**Scope**: All stored procedure executions
**Transaction Safety**: Ensures data consistency across retries

### 12.6 Parallel Processing Coordination

#### Group-Based Device Validation
**Implementation**: Multiple SQS messages for device groups
**Coordination**: Group number tracking prevents overlap
**Completion Detection**: `IsLastProcessDeviceNotExistsStaging` flag
**Resource Management**: Parallel Lambda executions for different groups

#### Service Provider Processing
**Sequential Processing**: One service provider at a time
**State Management**: Service provider ID tracking across executions
**Completion Detection**: Next service provider lookup returns -1

### 12.7 Data Consistency and State Management

#### Staging Table Strategy
**Persistence**: Staging data persists across Lambda executions
**Isolation**: Service provider data isolated in staging
**Cleanup**: Staging cleared only at service provider transitions

#### Processing State Tracking
**SQS Message Attributes**: Complete state preserved in queue messages
**Database State**: Processed flags prevent duplicate processing
**Retry State**: Retry counters track attempt history

#### Concurrency Control
**Database Locking**: ROWLOCK hints for concurrent access
**Queue Ordering**: FIFO processing where order matters
**Resource Limits**: Lambda concurrency controls prevent resource exhaustion

### 12.8 Monitoring and Observability

#### CloudWatch Integration
**Lambda Metrics**: Duration, errors, invocations automatically tracked
**Custom Metrics**: Processing counts, API call success rates
**Log Aggregation**: All Lambda logs centralized in CloudWatch

#### SQS Monitoring
**Queue Depth**: Number of pending messages
**Processing Rate**: Message consumption rates
**Dead Letter Queues**: Failed message tracking

#### Database Monitoring
**Connection Pool**: Database connection utilization
**Query Performance**: Slow query identification
**Lock Contention**: Database blocking and deadlock detection

---

## Conclusion

The TelegenceGetDevices Lambda represents a sophisticated, resilient data synchronization system with comprehensive error handling, state management, and orchestration capabilities. The system successfully handles the complexities of API pagination, database transactions, concurrent processing, and failure recovery while maintaining data consistency and processing efficiency.

Key strengths of the system include:
- **Robust retry mechanisms** at multiple levels (API, Lambda, Database)
- **Comprehensive state management** enabling seamless continuation processing
- **Efficient resource utilization** through timeout management and parallel processing
- **Detailed logging and monitoring** for operational visibility
- **Flexible configuration** supporting different environments and processing requirements

The orchestrated approach ensures reliable data synchronization from the Telegence Carrier API while providing the scalability and fault tolerance required for enterprise-grade data processing operations.
