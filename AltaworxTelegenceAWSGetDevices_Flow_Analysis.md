# AltaworxTelegenceAWSGetDevices Lambda Function Flow Analysis

## Overview
This document provides a comprehensive analysis of the AltaworxTelegenceAWSGetDevices.cs Lambda function, including high-level and detailed execution flows.

## Architecture
The lambda function is designed to sync Telegence device data and operates in multiple phases:
1. **Initialization Phase**: Get service provider and setup
2. **BAN Status Processing**: Get Billing Account Number statuses
3. **Device List Processing**: Retrieve and process device lists
4. **Device Not Exists Processing**: Handle devices that exist in AMOP but not in API
5. **Queue Management**: Send messages to other queues for further processing

---

## High-Level Flow

### Entry Point: `FunctionHandler`
```
FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
├── BaseFunctionHandler(context) → KeySysLambdaContext
├── Parse SQS Message Attributes → TelegenceGetDevicesSyncState
└── TryProcessDeviceListAsync(context, syncState)
```

### Main Processing Flow: `TryProcessDeviceListAsync`

#### Phase 1: Service Provider Setup
```
TryProcessDeviceListAsync
├── If CurrentServiceProviderId == 0:
│   ├── ServiceProviderCommon.GetNextServiceProviderId()
│   ├── TruncateTelegenceDeviceAndUsageStaging()
│   └── TruncateTelegenceBillingAccountNumberStatusStaging()
```

#### Phase 2: Processing Logic Branch
```
└── If IsProcessDeviceNotExistsStaging:
    ├── GetBanListStatusesStaging()
    └── ProcessDeviceNotExistsStagingAsync()
    
└── Else If InitializeProcessing:
    ├── ProcessBanListAsync()
    ├── SaveBillingAccountNumberStatusStaging()
    ├── GetBanList()
    └── SendMessageToGetDevicesQueueAsync() [with retry logic]
    
└── Else:
    ├── GetBanListStatusesStaging()
    ├── GetFANFilter()
    └── ProcessDeviceListAsync() OR CheckIfServiceProviderFirstSync()
```

---

## Detailed Sequential Flow

### 1. **FunctionHandler** (Entry Point)
**File**: AltaworxTelegenceAWSGetDevices.cs:48
**Purpose**: Lambda entry point, processes SQS messages

**Flow**:
1. Initialize `KeySysLambdaContext` via `BaseFunctionHandler()` (AWSFunctionBase.cs:40)
2. Load environment variables if not set
3. Parse SQS message attributes into `TelegenceGetDevicesSyncState`
4. Call `TryProcessDeviceListAsync()` for each record
5. Handle exceptions and cleanup via `CleanUp()`

**Key Attributes Parsed**:
- CurrentPage, HasMoreData, CurrentServiceProviderId
- InitializeProcessing, IsProcessDeviceNotExistsStaging
- GroupNumber, RetryNumber

---

### 2. **TryProcessDeviceListAsync** (Main Orchestrator)
**File**: AltaworxTelegenceAWSGetDevices.cs:179
**Purpose**: Main processing logic orchestrator

**Flow**:
```
TryProcessDeviceListAsync
├── Create PolicyFactory for retry logic
├── If CurrentServiceProviderId == 0:
│   ├── ServiceProviderCommon.GetNextServiceProviderId() [ServiceProviderCommon.cs:12]
│   ├── TruncateTelegenceDeviceAndUsageStaging() [Line 852]
│   └── TruncateTelegenceBillingAccountNumberStatusStaging() [Line 868]
│
├── Branch 1: IsProcessDeviceNotExistsStaging == true
│   ├── GetBanListStatusesStaging() [Line 391]
│   └── ProcessDeviceNotExistsStagingAsync() [Line 596]
│
├── Branch 2: InitializeProcessing == true
│   ├── ProcessBanListAsync() [Line 291]
│   ├── SaveBillingAccountNumberStatusStaging() [Line 418]
│   ├── GetBanList() [Line 349]
│   └── SendMessageToGetDevicesQueueAsync() [Line 884]
│
└── Branch 3: Normal Processing
    ├── GetBanListStatusesStaging() [Line 391]
    ├── GetFANFilter() [AWSFunctionBase.cs:431]
    ├── ProcessDeviceListAsync() [Line 440]
    └── OR CheckIfServiceProviderFirstSync() [Line 256]
```

---

### 3. **ProcessBanListAsync** (BAN Status Processing)
**File**: AltaworxTelegenceAWSGetDevices.cs:291
**Purpose**: Process Billing Account Number statuses from Telegence API

**Flow**:
```
ProcessBanListAsync
├── If RetryNumber == 0:
│   └── PrepareBanListToProcess() [Line 330]
│       └── SqlQueryHelper.ExecuteStoredProcedureWithRowCountResult()
│           └── USP_TELEGENCE_DEVICES_PREPARE_BANS_TO_PROCESS
│
├── GetBanList() [Line 349]
│   └── SqlQueryHelper.ExecuteStoredProcedureWithListResult()
│       └── GET_BAN_LIST_NEED_TO_PROCESS
│
├── TelegenceCommon.GetTelegenceAuthenticationInformation() [TelegenceCommon.cs:25]
│
├── For each BAN:
│   └── TelegenceCommon.GetBanStatusAsync() [TelegenceCommon.cs:472]
│       ├── GetBanStatusAsyncByProxy() OR GetBanStatusAsyncWithoutProxy()
│       └── Returns BAN status string
│
└── MarkProcessForEachBANProcessed() [Line 372]
    └── SqlQueryHelper.ExecuteStoredProcedureWithRowCountResult()
        └── MARK_PROCESSED_FOR_BAN
```

---

### 4. **ProcessDeviceListAsync** (Device Data Processing)
**File**: AltaworxTelegenceAWSGetDevices.cs:440
**Purpose**: Process device list from Telegence API

**Flow**:
```
ProcessDeviceListAsync
├── Initialize cycle counter and device list
│
├── While cycleCounter <= MaxCyclesToProcess AND !IsLastCycle:
│   ├── Check remaining execution time
│   ├── GetTelegenceDevicesFromAPI() [Line 532]
│   │   └── TelegenceCommon.GetTelegenceDevicesAsync() [TelegenceCommon.cs:438]
│   │       ├── GetTelegenceAuthenticationInformation()
│   │       ├── GetTelegenceDevicesAsyncByProxy() OR GetTelegenceDevicesAsyncWithoutProxy()
│   │       └── Parse response and update syncState
│   ├── Increment CurrentPage and cycleCounter
│   └── Break if IsLastCycle or time running out
│
├── If isFirstSync:
│   └── GetBANStatusFromAPI() [Line 575]
│       └── For each unique BAN in devices:
│           └── TelegenceCommon.GetBanStatusAsync()
│
├── SaveDevicesToStagingTable() [Line 538]
│   ├── Apply FAN filters (IncludedFANs/ExcludedFANs)
│   ├── Create DataTable with device data
│   └── SqlBulkCopy to TelegenceDeviceStaging
│
├── If IsLastCycle:
│   └── CheckForNextServiceProvider() [Line 521]
│       └── ServiceProviderCommon.GetNextServiceProviderId()
│
├── If more data OR retry needed:
│   └── SendMessageToGetDevicesQueueAsync() [Line 884]
│
└── Else:
    ├── GetTelegenceDeviceNotExistsStaging() [Line 796]
    ├── GetGroupCount() [Line 769]
    └── SendProcessDeviceNotExistsStagingMessagesToQueueAsync() [Line 751]
```

---

### 5. **ProcessDeviceNotExistsStagingAsync** (Missing Device Processing)
**File**: AltaworxTelegenceAWSGetDevices.cs:596
**Purpose**: Process devices that exist in AMOP but not in Telegence API

**Flow**:
```
ProcessDeviceNotExistsStagingAsync
├── GetTelegenceDeviceNotExistsOnStagingToProcess() [Line 699]
│   └── SqlQueryHelper.ExecuteStoredProcedureWithListResult()
│       └── GET_TELEGENCE_DEVICES_NOT_EXISTS_ON_STAGING
│
├── For each device needing check:
│   ├── Check remaining execution time
│   ├── TelegenceCommon.GetTelegenceAuthenticationInformation()
│   ├── TelegenceCommon.GetTelegenceDeviceBySubscriberNumber() [TelegenceCommon.cs:358]
│   │   ├── GetTelegenceDeviceBySubscriberNumberByProxy() OR
│   │   └── GetTelegenceDeviceBySubscriberNumberWithoutProxy()
│   ├── Parse device detail response
│   ├── Check if subscriber status changed
│   ├── Add to staging table if status differs and not cancelled
│   └── Add to processed list
│
├── If devices to insert:
│   └── SqlBulkCopy to TelegenceDeviceStaging
│
├── MarkProcessForEachDevicesHaveProcessed() [Line 720]
│   └── SqlQueryHelper.ExecuteStoredProcedureWithRowCountResult()
│       └── MARK_TELEGENCE_DEVICES_PROCESSED_ON_PROCESS_CHECK_EXISTS_ON_STAGING
│
├── Check remaining devices to process
│
├── If more devices AND retry allowed:
│   └── SendMessageToGetDevicesQueueAsync() [with staging flags]
│
└── If IsLastProcessDeviceNotExistsStaging:
    ├── SendMessageToGetDeviceUsageQueueAsync() [Line 927]
    └── SendMessageToGetDeviceDetailQueueAsync() [Line 954]
```

---

### 6. **CheckIfServiceProviderFirstSync** (First Sync Logic)
**File**: AltaworxTelegenceAWSGetDevices.cs:256
**Purpose**: Handle first-time sync for a service provider

**Flow**:
```
CheckIfServiceProviderFirstSync
├── GetTelegenceCurrentDeviceCount() [Line 273]
│   └── SqlQueryHelper.ExecuteStoredProcedureWithIntResult()
│       └── TELEGENCE_GET_CURRENT_DEVICES_COUNT
│
├── GetFANFilter() [AWSFunctionBase.cs:431]
│
├── If existingDeviceCount == 0:
│   └── ProcessDeviceListAsync() [with isFirstSync = true]
│
└── Else:
    └── Log warning about existing devices but no BAN
```

---

## Supporting Methods Detail

### Database Operations

#### **GetBanListStatusesStaging**
**File**: AltaworxTelegenceAWSGetDevices.cs:391
**Purpose**: Get BAN statuses from staging table
**Query**: `SELECT BillingAccountNumber, Status, ServiceProviderId FROM TelegenceDeviceBillingNumberAccountStatusStaging`

#### **TruncateTelegenceDeviceAndUsageStaging**
**File**: AltaworxTelegenceAWSGetDevices.cs:852
**Purpose**: Clear staging tables
**SP**: `usp_Telegence_Truncate_DeviceAndUsageStaging`

#### **TruncateTelegenceBillingAccountNumberStatusStaging**
**File**: AltaworxTelegenceAWSGetDevices.cs:868
**Purpose**: Clear BAN status staging table
**SP**: `usp_Telegence_Truncate_BillingAccountNumberStatusStaging`

#### **GetTelegenceDeviceNotExistsStaging**
**File**: AltaworxTelegenceAWSGetDevices.cs:796
**Purpose**: Populate devices not exists staging table
**SP**: `usp_GetTelegenDevice_NotExists_Stagging`

### API Operations (TelegenceCommon.cs)

#### **GetTelegenceAuthenticationInformation**
**File**: TelegenceCommon.cs:25
**Purpose**: Get Telegence API credentials
**SP**: `usp_Telegence_Get_AuthenticationByProviderId`

#### **GetTelegenceDevicesAsync**
**File**: TelegenceCommon.cs:438
**Purpose**: Get device list from Telegence API
**Endpoints**: Uses proxy or direct HTTP calls with pagination

#### **GetBanStatusAsync**
**File**: TelegenceCommon.cs:472
**Purpose**: Get BAN status from Telegence API
**Endpoints**: Uses proxy or direct HTTP calls

#### **GetTelegenceDeviceBySubscriberNumber**
**File**: TelegenceCommon.cs:358
**Purpose**: Get individual device details by subscriber number
**Endpoints**: Uses proxy or direct HTTP calls

### Queue Operations

#### **SendMessageToGetDevicesQueueAsync**
**File**: AltaworxTelegenceAWSGetDevices.cs:884
**Purpose**: Send message to continue device processing
**Queue**: TelegenceDestinationQueueGetDevicesURL

#### **SendMessageToGetDeviceUsageQueueAsync**
**File**: AltaworxTelegenceAWSGetDevices.cs:927
**Purpose**: Trigger device usage processing
**Queue**: TelegenceDeviceUsageQueueURL

#### **SendMessageToGetDeviceDetailQueueAsync**
**File**: AltaworxTelegenceAWSGetDevices.cs:954
**Purpose**: Trigger device detail processing
**Queue**: DeviceDetailQueueURL

---

## Key Dependencies

### ServiceProviderCommon.cs
- **GetNextServiceProviderId**: Get next service provider to process

### SqlQueryHelper.cs
- **ExecuteStoredProcedureWithListResult**: Execute SP returning list
- **ExecuteStoredProcedureWithRowCountResult**: Execute SP returning affected rows
- **ExecuteStoredProcedureWithIntResult**: Execute SP returning integer

### AWSFunctionBase.cs
- **BaseFunctionHandler**: Initialize lambda context
- **LogInfo**: Logging functionality
- **SqlBulkCopy**: Bulk insert to database
- **GetFANFilter**: Get Foundation Account Number filters

---

## State Management

### TelegenceGetDevicesSyncState
Key properties that control flow:
- **CurrentServiceProviderId**: Which service provider is being processed
- **CurrentPage**: Current page for API pagination
- **HasMoreData**: Whether more API pages exist
- **InitializeProcessing**: Whether in initialization phase
- **IsProcessDeviceNotExistsStaging**: Whether processing missing devices
- **IsLastProcessDeviceNotExistsStaging**: Whether this is the last missing device group
- **GroupNumber**: Group number for batch processing
- **RetryNumber**: Current retry attempt

---

## Error Handling & Retry Logic

### Retry Mechanisms
1. **SQL Operations**: PolicyFactory with configurable retry attempts
2. **API Calls**: Polly retry policies in TelegenceCommon
3. **Lambda Execution**: SQS message retry with DelaySeconds
4. **Time Management**: Checks `RemainingTime` to avoid timeouts

### Exception Handling
- All methods wrapped in try-catch blocks
- Exceptions logged with context information
- Failed operations trigger retry queues
- Database operations use transaction-safe patterns

---

## Performance Considerations

### Batch Processing
- **BatchSize**: Configurable batch size for API calls (default 250)
- **MaxCyclesToProcess**: Limits processing cycles per execution
- **Pagination**: Handles large datasets through API pagination
- **Bulk Operations**: Uses SqlBulkCopy for efficient database inserts

### Resource Management
- **Connection Pooling**: Proper disposal of database connections
- **HTTP Client**: Configured timeouts and proper disposal
- **Memory Management**: Streaming large datasets where possible

---

## Execution Paths Summary

### Path 1: Initial Service Provider Setup
`FunctionHandler` → `TryProcessDeviceListAsync` → `GetNextServiceProviderId` → `TruncateStaging` → `ProcessBanListAsync`

### Path 2: BAN Status Processing
`ProcessBanListAsync` → `GetBanList` → `GetBanStatusAsync` → `SaveBillingAccountNumberStatusStaging` → `SendMessageToGetDevicesQueueAsync`

### Path 3: Device List Processing
`ProcessDeviceListAsync` → `GetTelegenceDevicesAsync` → `SaveDevicesToStagingTable` → `SendMessageToGetDevicesQueueAsync` OR `ProcessDeviceNotExists`

### Path 4: Missing Device Processing
`ProcessDeviceNotExistsStagingAsync` → `GetTelegenceDeviceBySubscriberNumber` → `SaveToStaging` → `SendMessageToUsageQueue` + `SendMessageToDetailQueue`

### Path 5: First Sync Processing
`CheckIfServiceProviderFirstSync` → `GetTelegenceCurrentDeviceCount` → `ProcessDeviceListAsync` (with first sync flag)

This comprehensive flow ensures complete synchronization of Telegence device data while handling various edge cases, retries, and performance considerations.