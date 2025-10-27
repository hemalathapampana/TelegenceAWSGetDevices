# AltaworxTelegenceAWSGetDevices Lambda Function Flow Analysis

## Overview
This document provides a comprehensive analysis of the AltaworxTelegenceAWSGetDevices.cs Lambda function, detailing both high-level and low-level execution flows. The function is responsible for synchronizing Telegence device data from external APIs to staging tables.

---

## High-Level Flow

### 1. Entry Point
- **Method**: `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
- **Purpose**: Main Lambda entry point that processes SQS messages

### 2. Initialization Phase
- **Method**: `BaseFunctionHandler(context)` (from AWSFunctionBase)
- **Purpose**: Initialize Lambda context and environment variables

### 3. Message Processing
- **Method**: `TryProcessDeviceListAsync(keysysContext, syncState)`
- **Purpose**: Main processing logic for device synchronization

### 4. Service Provider Management
- **Method**: `ServiceProviderCommon.GetNextServiceProviderId()` (from ServiceProviderCommon)
- **Purpose**: Get next service provider to process

### 5. Data Truncation (Initial Processing)
- **Method**: `TruncateTelegenceDeviceAndUsageStaging()`
- **Method**: `TruncateTelegenceBillingAccountNumberStatusStaging()`
- **Purpose**: Clear staging tables for fresh data

### 6. BAN (Billing Account Number) Processing
- **Method**: `ProcessBanListAsync()`
- **Purpose**: Process billing account numbers and their statuses

### 7. Device List Processing
- **Method**: `ProcessDeviceListAsync()`
- **Purpose**: Fetch and process device data from Telegence API

### 8. Device Detail Processing
- **Method**: `ProcessDeviceNotExistsStagingAsync()`
- **Purpose**: Process devices that exist in AMOP but not in API response

### 9. Queue Management
- **Method**: `SendMessageToGetDevicesQueueAsync()`
- **Method**: `SendMessageToGetDeviceUsageQueueAsync()`
- **Method**: `SendMessageToGetDeviceDetailQueueAsync()`
- **Purpose**: Send messages to various queues for continued processing

### 10. Cleanup
- **Method**: `CleanUp(keysysContext)` (from AWSFunctionBase)
- **Purpose**: Clean up resources and context

---

## Sequential Method Flow

### Primary Execution Path:
1. `FunctionHandler()`
2. `BaseFunctionHandler()` → `TryProcessDeviceListAsync()`
3. `ServiceProviderCommon.GetNextServiceProviderId()`
4. `TruncateTelegenceDeviceAndUsageStaging()` + `TruncateTelegenceBillingAccountNumberStatusStaging()`
5. `ProcessBanListAsync()` → `SaveBillingAccountNumberStatusStaging()`
6. `ProcessDeviceListAsync()` → `GetTelegenceDevicesFromAPI()`
7. `SaveDevicesToStagingTable()` → `SqlBulkCopy()`
8. `GetTelegenceDeviceNotExistsStaging()` → `SendProcessDeviceNotExistsStagingMessagesToQueueAsync()`
9. `SendMessageToGetDeviceUsageQueueAsync()` + `SendMessageToGetDeviceDetailQueueAsync()`
10. `CleanUp()`

---

## Low-Level Flow Details

### 1. FunctionHandler Method
```csharp
public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
```
**What happens:**
- Initializes `KeySysLambdaContext` by calling `BaseFunctionHandler()`
- Loads environment variables (URLs, batch sizes, timeouts)
- Processes SQS event records or creates default sync state
- Extracts message attributes (CurrentPage, HasMoreData, ServiceProviderId, etc.)
- Calls `TryProcessDeviceListAsync()` for each record
- Handles exceptions and performs cleanup

### 2. BaseFunctionHandler Method (AWSFunctionBase.cs)
```csharp
public KeySysLambdaContext BaseFunctionHandler(ILambdaContext context, bool skipOUSpecificLogic = false)
```
**What happens:**
- Creates and initializes `KeySysLambdaContext`
- Loads OU (Organizational Unit) specific settings
- Sets up database connections and logging infrastructure
- Returns initialized context for use throughout the function

### 3. TryProcessDeviceListAsync Method
```csharp
private async Task TryProcessDeviceListAsync(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState)
```
**What happens:**
- Creates `PolicyFactory` for retry policies
- Determines next service provider if current is 0
- Calls `ServiceProviderCommon.GetNextServiceProviderId()` to get service provider
- Truncates staging tables if processing new service provider
- Branches into different processing paths:
  - **Initialize Processing**: Processes BAN (Billing Account Number) status
  - **Device Processing**: Processes device list from API
  - **Device Not Exists Processing**: Handles devices that exist in AMOP but not in API

### 4. ServiceProviderCommon.GetNextServiceProviderId (ServiceProviderCommon.cs)
```csharp
public static int GetNextServiceProviderId(string connectionString, IntegrationType integrationType, int currentServiceProviderId)
```
**What happens:**
- Executes stored procedure `usp_DeviceSync_Get_NextServiceProviderIdByIntegration`
- Passes current provider ID and integration type (Telegence)
- Returns next service provider ID to process
- Returns 0 on exception, -1 if no provider found

### 5. ProcessBanListAsync Method
```csharp
private async Task<Dictionary<string, string>> ProcessBanListAsync(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, PolicyFactory policyFactory)
```
**What happens:**
- Calls `PrepareBanListToProcess()` to prepare BAN list for processing
- Calls `GetBanList()` to retrieve BAN list from database
- Gets Telegence authentication using `TelegenceCommon.GetTelegenceAuthenticationInformation()`
- For each BAN, calls `TelegenceCommon.GetBanStatusAsync()` to get status from API
- Marks processed BANs using `MarkProcessForEachBANProcessed()`
- Returns dictionary of BAN statuses

### 6. PrepareBanListToProcess Method
```csharp
public void PrepareBanListToProcess(Action<string, string> logFunction, string connectionString, PolicyFactory policyFactory)
```
**What happens:**
- Uses `SqlQueryHelper.ExecuteStoredProcedureWithRowCountResult()`
- Executes `USP_TELEGENCE_DEVICES_PREPARE_BANS_TO_PROCESS` stored procedure
- Prepares billing account numbers for processing
- Uses retry policy for database operations

### 7. GetBanList Method
```csharp
private List<string> GetBanList(Action<string, string> logFunction, string centralDbConnectionString, PolicyFactory policyFactory)
```
**What happens:**
- Uses `SqlQueryHelper.ExecuteStoredProcedureWithListResult()`
- Executes `GET_BAN_LIST_NEED_TO_PROCESS` stored procedure
- Uses `GetBillingAccountNumberFromDatareader()` to parse results
- Returns list of billing account numbers to process

### 8. TelegenceCommon.GetTelegenceAuthenticationInformation (TelegenceCommon.cs)
```csharp
public static TelegenceAuthentication GetTelegenceAuthenticationInformation(string connectionString, int serviceProviderId)
```
**What happens:**
- Executes stored procedure `usp_Telegence_Get_AuthenticationByProviderId`
- Retrieves authentication details (ClientId, ClientSecret, URLs, credentials)
- Returns `TelegenceAuthentication` object with API credentials
- Returns null if no authentication found

### 9. TelegenceCommon.GetBanStatusAsync (TelegenceCommon.cs)
```csharp
public static async Task<string> GetBanStatusAsync(KeySysLambdaContext context, TelegenceAuthentication telegenceAuth, string proxyUrl, string ban, string telegenceBanDetailGetURL)
```
**What happens:**
- Constructs BAN detail URL by replacing {ban} placeholder
- Determines base URL (production vs sandbox)
- Calls either `GetBanStatusAsyncByProxy()` or `GetBanStatusAsyncWithoutProxy()`
- Makes HTTP GET request to Telegence API
- Deserializes response to get billing account status
- Returns status string

### 10. ProcessDeviceListAsync Method
```csharp
protected async Task ProcessDeviceListAsync(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, Dictionary<string, string> banStatus, Dictionary<string, List<string>> fanFilter = null, bool isFirstSync = false)
```
**What happens:**
- Initializes device list and cycle counter
- Loops through multiple API calls (up to MaxCyclesToProcess)
- For each cycle, calls `GetTelegenceDevicesFromAPI()`
- Increments page number and cycle counter
- Checks remaining execution time before each API call
- If first sync, gets BAN status from API using `GetBANStatusFromAPI()`
- Saves devices to staging table using `SaveDevicesToStagingTable()`
- Manages queue messages for continuation or next steps

### 11. GetTelegenceDevicesFromAPI Method
```csharp
protected virtual async Task<TelegenceGetDevicesSyncState> GetTelegenceDevicesFromAPI(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, List<TelegenceDeviceResponse> telegenceDeviceList)
```
**What happens:**
- Calls `TelegenceCommon.GetTelegenceDevicesAsync()`
- Passes context, sync state, proxy URL, device list, endpoint URL, and batch size
- Returns updated sync state with pagination information

### 12. TelegenceCommon.GetTelegenceDevicesAsync (TelegenceCommon.cs)
```csharp
public static async Task<TelegenceGetDevicesSyncState> GetTelegenceDevicesAsync(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, string proxyUrl, List<TelegenceDeviceResponse> telegenceDeviceList, string deviceDetailEndpoint, int pageSize)
```
**What happens:**
- Gets Telegence authentication information
- Determines base URL (production vs sandbox)
- Calls either `GetTelegenceDevicesAsyncByProxy()` or `GetTelegenceDevicesAsyncWithoutProxy()`
- Makes paginated HTTP GET requests to Telegence devices API
- Adds pagination headers (current-page, page-size)
- Deserializes JSON response to device list
- Updates sync state with pagination information (HasMoreData, IsLastCycle)
- Adds refresh timestamp to each device
- Returns updated sync state

### 13. SaveDevicesToStagingTable Method
```csharp
protected virtual void SaveDevicesToStagingTable(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, Dictionary<string, string> banStatus, List<TelegenceDeviceResponse> telegenceDeviceList, Dictionary<string, List<string>> fanFilter = null)
```
**What happens:**
- Applies FAN (Foundation Account Number) filtering if configured
- Creates DataTable with device columns
- For each device, calls `GetBanStatusTextForDevice()` to get BAN status
- Calls `AddToDataRow()` to populate DataTable row
- Uses `SqlBulkCopy()` to insert data into `TelegenceDeviceStaging` table

### 14. SqlBulkCopy Method (AWSFunctionBase.cs)
```csharp
public static void SqlBulkCopy(KeySysLambdaContext context, string connectionString, DataTable table, string tableName, List<SqlBulkCopyColumnMapping> columnMappings = null)
```
**What happens:**
- Opens SQL connection
- Creates `SqlBulkCopy` object with destination table name
- Sets timeout and batch size from constants
- Applies column mappings if provided
- Executes bulk insert operation
- Handles SQL exceptions and logs results

### 15. GetTelegenceDeviceNotExistsStaging Method
```csharp
protected virtual void GetTelegenceDeviceNotExistsStaging(KeySysLambdaContext context)
```
**What happens:**
- Executes stored procedure `usp_GetTelegenDevice_NotExists_Stagging`
- Passes BatchSize parameter
- Identifies devices that exist in AMOP but not in API response
- Prepares them for individual processing

### 16. ProcessDeviceNotExistsStagingAsync Method
```csharp
private async Task ProcessDeviceNotExistsStagingAsync(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, Dictionary<string, string> banStatus, PolicyFactory policyFactory)
```
**What happens:**
- Gets devices to process using `GetTelegenceDeviceNotExistsOnStagingToProcess()`
- For each device, makes individual API call using `TelegenceCommon.GetTelegenceDeviceBySubscriberNumber()`
- Deserializes device detail response
- Checks if subscriber status has changed
- Adds updated devices to staging table
- Marks processed devices using `MarkProcessForEachDevicesHaveProcessed()`
- Continues processing or triggers next phase

### 17. TelegenceCommon.GetTelegenceDeviceBySubscriberNumber (TelegenceCommon.cs)
```csharp
public static async Task<string> GetTelegenceDeviceBySubscriberNumber(KeySysLambdaContext context, TelegenceAuthentication telegenceAuthentication, bool isProduction, string subscriberNo, string endpoint, string proxyUrl)
```
**What happens:**
- Constructs device detail endpoint with subscriber number
- Determines base URL (production vs sandbox)
- Calls either proxy or direct HTTP methods
- Makes HTTP GET request to get individual device details
- Returns JSON response string
- Handles retries and error responses

### 18. SendMessageToGetDevicesQueueAsync Method
```csharp
protected virtual async Task SendMessageToGetDevicesQueueAsync(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, string telegenceDestinationQueueGetDevicesURL, int delaySeconds, int groupNumber = 0, int isProcessDeviceNotExistsStaging = 0, int isLastGroup = 0)
```
**What happens:**
- Creates Amazon SQS client with AWS credentials
- Builds SQS message with sync state attributes
- Sets delay seconds for message processing
- Includes pagination and processing flags
- Sends message to continuation queue
- Logs response status

### 19. SendMessageToGetDeviceUsageQueueAsync Method
```csharp
protected virtual async Task SendMessageToGetDeviceUsageQueueAsync(KeySysLambdaContext context, string deviceUsageQueueURL, int delaySeconds)
```
**What happens:**
- Creates SQS client and message for device usage processing
- Sets initialization flag to true
- Sends message to device usage queue
- Triggers next phase of processing pipeline

### 20. SendMessageToGetDeviceDetailQueueAsync Method
```csharp
private async Task SendMessageToGetDeviceDetailQueueAsync(KeySysLambdaContext context, string deviceDetailQueueURL, int delaySeconds)
```
**What happens:**
- Creates SQS message for device detail processing
- Sets group number to 0 for initial processing
- Sends message with delay to device detail queue
- Completes the processing pipeline

### 21. SqlQueryHelper Methods (SqlQueryHelper.cs)

#### ExecuteStoredProcedureWithListResult
**What happens:**
- Validates input parameters
- Opens SQL connection and creates command
- Sets command type to StoredProcedure
- Adds parameters and sets timeout
- Executes command and reads results
- Uses provided parse function to convert SqlDataReader to objects
- Returns list of parsed objects
- Handles SQL exceptions with retry policies

#### ExecuteStoredProcedureWithRowCountResult
**What happens:**
- Similar setup to list result method
- Executes `ExecuteNonQuery()` instead of `ExecuteReader()`
- Returns number of affected rows
- Logs row count information
- Used for insert/update/delete operations

#### ExecuteStoredProcedureWithIntResult
**What happens:**
- Executes stored procedure expecting single integer result
- Uses `ExecuteScalar()` to get single value
- Converts result to integer with default value handling
- Used for getting counts or IDs from database

### 22. CleanUp Method (AWSFunctionBase.cs)
```csharp
public virtual void CleanUp(KeySysLambdaContext context)
```
**What happens:**
- Calls context cleanup methods
- Disposes database connections
- Releases logging resources
- Ensures proper resource disposal

---

## Key Processing Patterns

### 1. Retry Patterns
- Uses Polly retry policies for HTTP requests and database operations
- Configurable retry counts via constants
- Exponential backoff for failed operations

### 2. Pagination Handling
- Processes devices in pages with configurable batch sizes
- Maintains page state across Lambda invocations
- Uses SQS message attributes for state persistence

### 3. Time-based Processing Control
- Monitors Lambda remaining execution time
- Stops processing when time threshold reached
- Queues continuation messages for next invocation

### 4. Error Handling
- Comprehensive exception handling at each level
- Detailed logging for troubleshooting
- Graceful degradation when services unavailable

### 5. Data Flow Management
- Staging tables for temporary data storage
- Bulk insert operations for performance
- State management across multiple Lambda invocations

---

## Database Operations Summary

### Stored Procedures Called:
1. `usp_DeviceSync_Get_NextServiceProviderIdByIntegration`
2. `usp_Telegence_Get_AuthenticationByProviderId`
3. `USP_TELEGENCE_DEVICES_PREPARE_BANS_TO_PROCESS`
4. `GET_BAN_LIST_NEED_TO_PROCESS`
5. `MARK_PROCESSED_FOR_BAN`
6. `TELEGENCE_GET_CURRENT_DEVICES_COUNT`
7. `GET_TELEGENCE_DEVICES_NOT_EXISTS_ON_STAGING`
8. `MARK_TELEGENCE_DEVICES_PROCESSED_ON_PROCESS_CHECK_EXISTS_ON_STAGING`
9. `usp_Telegence_Truncate_DeviceAndUsageStaging`
10. `usp_Telegence_Truncate_BillingAccountNumberStatusStaging`
11. `usp_GetTelegenDevice_NotExists_Stagging`

### Tables Accessed:
1. `TelegenceDeviceStaging` (bulk insert)
2. `TelegenceDeviceBillingNumberAccountStatusStaging` (read/write)
3. `TelegenceDeviceNotExistsStagingToProcess` (read)
4. Various service provider and authentication tables

---

## API Endpoints Used

### Telegence API Calls:
1. **Device List**: GET request with pagination headers
2. **BAN Status**: GET request for billing account status
3. **Device Detail**: GET request for individual device details

### HTTP Methods:
- All external API calls use GET requests
- Proxy support for environments requiring it
- Authentication via app-id and app-secret headers

---

## Queue Management

### SQS Queues Used:
1. **TelegenceDestinationQueueGetDevicesURL**: Continuation of device processing
2. **TelegenceDeviceUsageQueueURL**: Device usage processing
3. **TelegenceDeviceDetailQueueURL**: Device detail processing

### Message Attributes:
- CurrentPage, HasMoreData, CurrentServiceProviderId
- InitializeProcessing, IsProcessDeviceNotExistsStaging
- GroupNumber, IsLastProcessDeviceNotExistsStaging, RetryNumber

This comprehensive flow ensures reliable, scalable processing of Telegence device data with proper error handling, state management, and continuation across multiple Lambda invocations.