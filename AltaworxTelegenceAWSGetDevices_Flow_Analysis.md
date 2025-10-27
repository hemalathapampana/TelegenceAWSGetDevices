# AltaworxTelegenceAWSGetDevices Lambda Function Flow Analysis

## Overview
This document provides a comprehensive flow analysis of the `AltaworxTelegenceAWSGetDevices` Lambda function, which is responsible for retrieving and processing Telegence device data from APIs and storing it in staging tables for further processing.

## High-Level Flow

### 1. Entry Point: `FunctionHandler`
- **Location**: AltaworxTelegenceAWSGetDevices.cs:48
- **Purpose**: Main Lambda entry point that processes SQS events
- **Next**: Calls `BaseFunctionHandler`

### 2. Base Initialization: `BaseFunctionHandler` 
- **Location**: AWSFunctionBase.cs:40
- **Purpose**: Initializes KeySysLambdaContext and OU-specific logic
- **Next**: Returns to `FunctionHandler` for processing

### 3. Core Processing: `TryProcessDeviceListAsync`
- **Location**: AltaworxTelegenceAWSGetDevices.cs:179
- **Purpose**: Main orchestrator for device list processing logic
- **Next**: Branches based on sync state conditions

### 4. Service Provider Management: `GetNextServiceProviderId`
- **Location**: ServiceProviderCommon.cs:12
- **Purpose**: Retrieves the next service provider to process
- **Next**: Returns service provider ID or error codes

### 5. Staging Cleanup: `TruncateTelegenceDeviceAndUsageStaging` & `TruncateTelegenceBillingAccountNumberStatusStaging`
- **Location**: AltaworxTelegenceAWSGetDevices.cs:852, 868
- **Purpose**: Clears staging tables for fresh data
- **Next**: Continues with BAN processing or device processing

### 6. BAN Status Processing: `ProcessBanListAsync`
- **Location**: AltaworxTelegenceAWSGetDevices.cs:291
- **Purpose**: Processes Billing Account Number status from API
- **Next**: Calls multiple helper methods for BAN processing

### 7. BAN List Retrieval: `GetBanList`
- **Location**: AltaworxTelegenceAWSGetDevices.cs:349
- **Purpose**: Gets list of BANs that need processing
- **Next**: Returns list to `ProcessBanListAsync`

### 8. Authentication Retrieval: `GetTelegenceAuthenticationInformation`
- **Location**: TelegenceCommon.cs:25
- **Purpose**: Gets authentication credentials for Telegence API
- **Next**: Returns authentication object

### 9. BAN Status API Call: `GetBanStatusAsync`
- **Location**: TelegenceCommon.cs:472
- **Purpose**: Makes API call to get BAN status
- **Next**: Returns status string

### 10. BAN Processing Completion: `MarkProcessForEachBANProcessed`
- **Location**: AltaworxTelegenceAWSGetDevices.cs:372
- **Purpose**: Marks BANs as processed in database
- **Next**: Updates database via `ExecuteStoredProcedureWithRowCountResult`

### 11. BAN Status Storage: `SaveBillingAccountNumberStatusStaging`
- **Location**: AltaworxTelegenceAWSGetDevices.cs:418
- **Purpose**: Saves BAN status data to staging table
- **Next**: Uses `SqlBulkCopy` for bulk insert

### 12. Staging BAN Retrieval: `GetBanListStatusesStaging`
- **Location**: AltaworxTelegenceAWSGetDevices.cs:391
- **Purpose**: Gets BAN statuses from staging table
- **Next**: Returns dictionary of BAN statuses

### 13. FAN Filtering: `GetFANFilter`
- **Location**: AWSFunctionBase.cs:431
- **Purpose**: Gets Foundation Account Number filters for service provider
- **Next**: Returns include/exclude lists

### 14. Device List Processing: `ProcessDeviceListAsync`
- **Location**: AltaworxTelegenceAWSGetDevices.cs:440
- **Purpose**: Main device processing orchestrator
- **Next**: Calls API and processes device data

### 15. Device API Call: `GetTelegenceDevicesAsync`
- **Location**: TelegenceCommon.cs:438
- **Purpose**: Makes API call to retrieve device list
- **Next**: Returns updated sync state with device data

### 16. Device Detail Processing: `ProcessDeviceNotExistsStagingAsync`
- **Location**: AltaworxTelegenceAWSGetDevices.cs:596
- **Purpose**: Processes devices that exist in AMOP but not in API
- **Next**: Gets device details and updates staging

### 17. Device Detail Retrieval: `GetTelegenceDeviceNotExistsOnStagingToProcess`
- **Location**: AltaworxTelegenceAWSGetDevices.cs:699
- **Purpose**: Gets devices that need detail checking
- **Next**: Returns list via `ExecuteStoredProcedureWithListResult`

### 18. Individual Device Detail: `GetTelegenceDeviceBySubscriberNumber`
- **Location**: TelegenceCommon.cs:358
- **Purpose**: Gets detailed device information by subscriber number
- **Next**: Returns device detail JSON

### 19. Device Processing Completion: `MarkProcessForEachDevicesHaveProcessed`
- **Location**: AltaworxTelegenceAWSGetDevices.cs:720
- **Purpose**: Marks devices as processed in database
- **Next**: Updates database via `ExecuteStoredProcedureWithRowCountResult`

### 20. Queue Management: `SendMessageToGetDevicesQueueAsync`
- **Location**: AltaworxTelegenceAWSGetDevices.cs:884
- **Purpose**: Sends continuation messages to SQS queue
- **Next**: Continues processing or triggers next phase

### 21. First Sync Check: `CheckIfServiceProviderFirstSync`
- **Location**: AltaworxTelegenceAWSGetDevices.cs:256
- **Purpose**: Determines if this is the first sync for service provider
- **Next**: Branches processing logic based on sync status

## Low-Level Flow Details

### Phase 1: Initialization and Setup

#### 1.1 FunctionHandler Entry
```csharp
public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
```
**What happens:**
- Initializes `KeySysLambdaContext` via `BaseFunctionHandler`
- Loads environment variables from Lambda context or client context
- Parses SQS message attributes into `TelegenceGetDevicesSyncState` object
- Extracts sync state parameters:
  - `CurrentPage`: Current page number for API pagination
  - `HasMoreData`: Boolean indicating if more data is available
  - `CurrentServiceProviderId`: ID of service provider being processed
  - `InitializeProcessing`: Boolean for initialization phase
  - `IsProcessDeviceNotExistsStaging`: Boolean for device existence check phase
  - `GroupNumber`: Batch group number for parallel processing
  - `RetryNumber`: Current retry attempt number
- Calls `TryProcessDeviceListAsync` with parsed sync state

#### 1.2 BaseFunctionHandler Setup
```csharp
public KeySysLambdaContext BaseFunctionHandler(ILambdaContext context, bool skipOUSpecificLogic = false)
```
**What happens:**
- Creates new `KeySysLambdaContext` from Lambda context
- Initializes logging infrastructure
- Loads organizational unit specific settings
- Sets up database connection strings
- Returns initialized context for use throughout the function

### Phase 2: Service Provider Management

#### 2.1 Service Provider Selection
```csharp
var serviceProvider = ServiceProviderCommon.GetNextServiceProviderId(context.CentralDbConnectionString, Amop.Core.Models.IntegrationType.Telegence, syncState.CurrentServiceProviderId)
```
**What happens:**
- Executes stored procedure `usp_DeviceSync_Get_NextServiceProviderIdByIntegration`
- Passes current service provider ID and Telegence integration type
- Returns next available service provider ID or special codes:
  - `0`: Exception occurred
  - `-1`: No authentication record found
  - `>0`: Valid service provider ID
- If valid ID returned, proceeds to truncate staging tables

#### 2.2 Staging Table Cleanup
**TruncateTelegenceDeviceAndUsageStaging:**
- Executes stored procedure `usp_Telegence_Truncate_DeviceAndUsageStaging`
- Clears all existing device and usage staging data
- Prepares for fresh data load

**TruncateTelegenceBillingAccountNumberStatusStaging:**
- Executes stored procedure `usp_Telegence_Truncate_BillingAccountNumberStatusStaging`
- Clears all existing BAN status staging data
- Prepares for fresh BAN status data

### Phase 3: BAN (Billing Account Number) Processing

#### 3.1 BAN List Preparation
```csharp
PrepareBanListToProcess(ParameterizedLog(context), context.CentralDbConnectionString, policyFactory)
```
**What happens:**
- Executes stored procedure `USP_TELEGENCE_DEVICES_PREPARE_BANS_TO_PROCESS`
- Prepares database records for BAN processing
- Marks BANs that need status updates

#### 3.2 BAN List Retrieval
```csharp
List<string> banList = GetBanList(ParameterizedLog(context), context.CentralDbConnectionString, policyFactory)
```
**What happens:**
- Executes stored procedure `GET_BAN_LIST_NEED_TO_PROCESS`
- Returns list of BAN numbers that need status updates
- Uses `SqlQueryHelper.ExecuteStoredProcedureWithListResult` with retry policy

#### 3.3 Authentication Information Retrieval
```csharp
var telegenceAuth = TelegenceCommon.GetTelegenceAuthenticationInformation(context.CentralDbConnectionString, syncState.CurrentServiceProviderId)
```
**What happens:**
- Executes stored procedure `usp_Telegence_Get_AuthenticationByProviderId`
- Retrieves Telegence API credentials including:
  - Client ID and Client Secret
  - Production and Sandbox URLs
  - Username and Password
  - Write permissions and bill period settings
- Returns `TelegenceAuthentication` object or null if not found

#### 3.4 BAN Status API Processing
```csharp
string status = await TelegenceCommon.GetBanStatusAsync(context, telegenceAuth, ProxyUrl, ban, TelegenceBanDetailGetURL)
```
**What happens:**
- Constructs API endpoint URL by replacing `{ban}` placeholder with actual BAN
- Determines base URL (production vs sandbox) based on environment
- Makes HTTP GET request to Telegence API:
  - **With Proxy**: Uses proxy service with authentication headers
  - **Without Proxy**: Direct HTTP call with app-id and app-secret headers
- Implements retry policy with `PollyRetryHttpRequestAsync`
- Parses JSON response to extract billing account status
- Returns status string or empty string on failure

#### 3.5 BAN Status Storage
```csharp
SaveBillingAccountNumberStatusStaging(context, banStatus, syncState.CurrentServiceProviderId)
```
**What happens:**
- Creates `DataTable` with columns: Id, BillingAccountNumber, Status, ServiceProviderId
- Populates table with BAN status data from API responses
- Uses `SqlBulkCopy` to insert data into `TelegenceDeviceBillingNumberAccountStatusStaging` table
- Bulk insert provides high performance for large datasets

#### 3.6 BAN Processing Completion
```csharp
MarkProcessForEachBANProcessed(ParameterizedLog(context), context.CentralDbConnectionString, banStatus.Select(x => x.Key).ToList(), policyFactory)
```
**What happens:**
- Executes stored procedure `MARK_PROCESSED_FOR_BAN`
- Passes comma-separated list of processed BAN numbers
- Updates database to mark these BANs as processed
- Prevents reprocessing in subsequent runs

### Phase 4: Device List Processing

#### 4.1 Device List API Call
```csharp
syncState = await GetTelegenceDevicesFromAPI(context, syncState, telegenceDeviceList)
```
**What happens:**
- Calls `TelegenceCommon.GetTelegenceDevicesAsync` with pagination parameters
- Constructs API request with headers:
  - `app-id`: Client ID from authentication
  - `app-secret`: Client secret from authentication  
  - `current-page`: Current page number
  - `page-size`: Batch size for pagination
- Makes HTTP GET request to device list endpoint
- Processes response:
  - Deserializes JSON to `List<TelegenceDeviceResponse>`
  - Extracts pagination headers (`page-total`, `refresh-timestamp`)
  - Updates sync state with pagination information
  - Adds devices to master list with refresh timestamp

#### 4.2 FAN Filtering
```csharp
var fanFilter = GetFANFilter(context, syncState.CurrentServiceProviderId)
```
**What happens:**
- Queries `ServiceProviderSetting` table for FAN filters
- Retrieves `IncludedFANs` and `ExcludedFANs` settings
- Parses comma/semicolon-separated values into lists
- Returns dictionary with include/exclude lists for filtering

#### 4.3 Device Data Processing and Storage
```csharp
SaveDevicesToStagingTable(context, syncState, banStatus, telegenceDeviceList, fanFilter)
```
**What happens:**
- Applies FAN filtering to device list:
  - If `IncludedFANs` specified, only include devices with matching FANs
  - If `ExcludedFANs` specified, exclude devices with matching FANs
- Creates `DataTable` with device staging schema:
  - ServiceProviderId, FoundationAccountNumber, BillingAccountNumber
  - SubscriberNumber, SubscriberNumberStatus, RefreshTimestamp
  - CreatedDate, BanStatus
- For each device:
  - Gets BAN status from previously retrieved BAN status data
  - Populates data row with device information
  - Adds current UTC timestamp as created date
- Uses `SqlBulkCopy` to insert into `TelegenceDeviceStaging` table

### Phase 5: Device Existence Verification

#### 5.1 Device Not Exists Staging Setup
```csharp
GetTelegenceDeviceNotExistsStaging(context)
```
**What happens:**
- Executes stored procedure `usp_GetTelegenDevice_NotExists_Stagging`
- Identifies devices that exist in AMOP but not returned by API
- Groups devices into batches for parallel processing
- Creates records in `TelegenceDeviceNotExistsStagingToProcess` table

#### 5.2 Device Existence Processing
```csharp
await ProcessDeviceNotExistsStagingAsync(context, syncState, banStatus, policyFactory)
```
**What happens:**
- Retrieves devices from staging table by group number
- For each device, makes individual API call to get device details
- Calls `GetTelegenceDeviceBySubscriberNumber` for detailed device information
- Compares API status with database status
- If status differs and not cancelled, adds to update list
- Marks processed devices in database
- Continues until all groups processed

#### 5.3 Individual Device Detail Retrieval
```csharp
string resultAPI = await TelegenceCommon.GetTelegenceDeviceBySubscriberNumber(context, telegenceAuthenticationInfo, context.IsProduction, telegenceDevice.SubscriberNumber, TelegenceDeviceDetailGetURL, ProxyUrl)
```
**What happens:**
- Constructs device detail endpoint with subscriber number
- Makes HTTP GET request to device detail API
- Deserializes JSON response to `TelegenceMobilityLineConfigurationResponse`
- Extracts `subscriberStatus` from service characteristics
- Compares with existing status and updates if different
- Skips devices with cancelled status ("C")

### Phase 6: Queue Management and Continuation

#### 6.1 Message Queue Processing
```csharp
await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, delaySeconds)
```
**What happens:**
- Creates SQS message with current sync state
- Includes message attributes:
  - `HasMoreData`: Pagination continuation flag
  - `CurrentPage`: Next page to process
  - `CurrentServiceProviderId`: Current service provider
  - `InitializeProcessing`: Phase indicator
  - `IsProcessDeviceNotExistsStaging`: Processing mode flag
  - `GroupNumber`: Batch group for parallel processing
  - `RetryNumber`: Current retry attempt
- Sets delay seconds for message processing
- Sends message to continuation queue for next processing cycle

#### 6.2 Workflow Orchestration
**Initialization Phase:**
- Processes BAN status updates
- Continues until all BANs processed or retry limit reached
- Transitions to device processing phase

**Device Processing Phase:**
- Retrieves and processes device lists with pagination
- Continues until all pages processed or retry limit reached
- Transitions to device existence verification

**Device Existence Phase:**
- Processes devices in parallel groups
- Verifies device status for discrepancies
- Completes with usage and detail queue triggers

### Phase 7: Completion and Next Phase Triggers

#### 7.1 Usage Queue Trigger
```csharp
await SendMessageToGetDeviceUsageQueueAsync(context, TelegenceDeviceUsageQueueURL, delaySeconds)
```
**What happens:**
- Sends message to device usage processing queue
- Triggers next phase of data processing pipeline
- Includes initialization flag for usage processing

#### 7.2 Detail Queue Trigger
```csharp
await SendMessageToGetDeviceDetailQueueAsync(context, DeviceDetailQueueURL, delayQueue)
```
**What happens:**
- Sends message to device detail processing queue
- Includes delay to allow usage processing to complete first
- Triggers final phase of device data enrichment

## Error Handling and Resilience

### Retry Mechanisms
- **SQL Operations**: Polly retry policy with configurable attempts
- **API Calls**: HTTP retry policy with exponential backoff
- **Queue Processing**: SQS retry with increasing retry numbers
- **Lambda Timeouts**: Monitors remaining execution time and gracefully stops

### Exception Management
- **SQL Exceptions**: Logged with error codes and retry attempts
- **HTTP Exceptions**: Logged with response details and retry attempts  
- **General Exceptions**: Full stack trace logging with context
- **Graceful Degradation**: Continues processing other records on individual failures

### Data Consistency
- **Transactional Operations**: Database operations wrapped in appropriate transaction scopes
- **Staging Tables**: Truncated at start to ensure clean data
- **Processing Markers**: Database flags prevent duplicate processing
- **State Management**: SQS message attributes maintain processing state across invocations

## Performance Optimizations

### Bulk Operations
- **SqlBulkCopy**: High-performance bulk inserts for staging data
- **Batch Processing**: Configurable batch sizes for API calls and database operations
- **Parallel Processing**: Device existence checks processed in parallel groups

### Resource Management
- **Connection Pooling**: Efficient database connection management
- **HTTP Client Reuse**: Proper disposal and timeout configuration
- **Memory Management**: Streaming data processing where possible

### Monitoring and Observability
- **Comprehensive Logging**: Detailed logging at each processing step
- **Performance Metrics**: Timing and count information for operations
- **Error Tracking**: Exception details with context for troubleshooting

This flow provides a complete picture of how the Telegence device synchronization process works, from initial API calls through data staging and verification, with robust error handling and performance optimizations throughout.