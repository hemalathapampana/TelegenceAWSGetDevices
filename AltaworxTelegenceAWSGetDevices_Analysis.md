# AltaworxTelegenceAWSGetDevices Lambda Function Analysis

## Overview
This document provides comprehensive answers to queries about the AltaworxTelegenceAWSGetDevices Lambda function based on code analysis.

## Trigger & Setup

### What makes the trigger happen?
**Answer**: The Lambda function is triggered by SQS events containing device synchronization messages. The trigger occurs when:
1. **Initial trigger**: A message is sent to the SQS queue to start the device synchronization process
2. **Self-triggering**: The Lambda re-enqueues itself with different parameters to continue processing:
   - For pagination (lines 503, 229, 683)
   - For retry scenarios (lines 221, 676)
   - For missing device processing (line 765)
   - To trigger Usage and Detail Lambda functions (lines 693-694)

The trigger mechanism uses SQS message attributes to maintain state across invocations, including CurrentPage, HasMoreData, CurrentServiceProviderId, InitializeProcessing, RetryNumber, etc.

## Processing Paths

### 1. SQL Retry Initialization

#### Why is retry first step?
**Answer**: Retry initialization is NOT the first step. The code analysis shows the actual flow:

1. **Service Provider Identification** (lines 185-204): First step is to identify the next service provider ID
2. **Staging Table Clearing** (lines 199-200): Only happens when getting a new service provider
3. **Processing Logic**: Then determines which path to take based on flags

The retry logic is embedded throughout the process using `PolicyFactory` with SQL retry policies that implement exponential backoff using the Polly library.

#### Are we sure device tables are getting cleared?
**Answer**: YES, device staging tables are definitely cleared, but with specific conditions:
- **When**: Only when starting with a new service provider (`syncState.CurrentServiceProviderId == 0`)
- **What gets cleared**: 
  - Device and Usage staging tables via `usp_Telegence_Truncate_DeviceAndUsageStaging` (line 199)
  - BAN status staging via `usp_Telegence_Truncate_BillingAccountNumberStatusStaging` (line 200)
- **Why not cleared on previous day completion**: The staging tables are only cleared when starting a new service provider sync, not at the end of each day's processing

#### Table name for BAN, FAN, Number, statuses storage:
**Answer**: `TelegenceDeviceBillingNumberAccountStatusStaging` (line 398)
- Stores: BillingAccountNumber, Status, ServiceProviderId
- Used for: Staging BAN status information before device processing

### 2. Normal Flow

#### Retrieves BAN list statuses from where?
**Answer**: BAN list statuses are retrieved from two sources:
1. **Database staging table**: `TelegenceDeviceBillingNumberAccountStatusStaging` (line 398)
2. **Telegence API**: When staging table is empty or for first sync, calls Telegence BAN Detail API

#### API details for device list fetching:
**Answer**: 
- **Endpoint**: `TelegenceDevicesGetURL` environment variable
- **Method**: GET request via `TelegenceCommon.GetTelegenceDevicesAsync`
- **Authentication**: Uses app-id and app-secret headers
- **Pagination**: Uses CurrentPage and PageSize headers
- **Response**: Returns device list with pagination headers (PageTotal, RefreshTimestamp)

#### What is the page limit?
**Answer**: 
- **Page Size**: Controlled by `BatchSize` environment variable (default: 250) - line 35
- **Max Cycles**: Controlled by `MaxCyclesToProcess` environment variable - line 28
- **Limit Logic**: Process continues until `MaxCyclesToProcess` reached OR `IsLastCycle` flag is true from API response

#### How to determine if pages are complete?
**Answer**: Pages are complete when:
1. **API Response**: `PageTotal` header indicates current page >= total pages (line 567-570)
2. **IsLastCycle flag**: Set to true when no more data available (line 572)
3. **HasMoreData flag**: Set to false when pagination complete
4. **Fallback**: `MaxCyclesToProcess` prevents infinite loops

### 3. Missing Devices Handling

#### API details for subscriber-level validation:
**Answer**:
- **Endpoint**: `TelegenceDeviceDetailGetURL` + subscriberNumber
- **Method**: GET request via `TelegenceCommon.GetTelegenceDeviceBySubscriberNumber`
- **Purpose**: Validates individual devices that exist in AMOP but missing from device list API
- **Response**: Returns `TelegenceMobilityLineConfigurationResponse` with device details

#### What happens to devices which are not validated?
**Answer**: 
- **Processed but not added**: Devices that fail validation are still marked as processed (line 654)
- **Status filtering**: Devices with status "C" (cancelled) are ignored (line 637)
- **Tracking**: All processed devices are tracked in `listDevicesProcessed` regardless of validation outcome
- **Cleanup**: Failed validations are logged but don't block the process

## Error Handling & Retry

### How does exponential backoff work?
**Answer**: Exponential backoff is implemented through:
1. **Polly Library**: Uses `RetryPolicyHelper.PollyRetryHttpRequestAsync` and `PollyRetryForProxyRequestAsync`
2. **SQL Retry**: `PolicyFactory.SqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES)`
3. **Lambda Retry**: Uses `RetryNumber` field with `CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES` limit
4. **Delay Mechanism**: `CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS` between retries

### Detail retry re-enqueuing:
**Answer**: Messages are re-enqueued when:
- **Incomplete processing**: `remainingBanNeedToProcesses.Count > 0` (line 221)
- **More pages available**: `!syncState.IsLastCycle || syncState.HasMoreData` (line 501)
- **Missing devices remaining**: `remainingDevicesNeedToProcesses.Count > 0` (line 676)
- **Timeout conditions**: When `RemainingTime < REMAINING_TIME_CUT_OFF` (lines 311, 457, 619)

## Reference Table - Detailed Usage

### Main Functions
- **FunctionHandler** (line 48): Entry point, processes SQS events and initializes context
- **TryProcessDeviceListAsync** (line 179): Main orchestration logic with error handling
- **ProcessDeviceListAsync** (line 440): Core device processing workflow

### Init Functions
- **BaseFunctionHandler**: Initializes Lambda context and configuration
- **ServiceProviderCommon.GetNextServiceProviderId** (line 187): Identifies next service provider to process using stored procedure `usp_DeviceSync_Get_NextServiceProviderIdByIntegration`

### Device Fetch
- **TelegenceCommon.GetTelegenceDevicesAsync** (line 535): Fetches paginated device list from Telegence API with retry logic and proxy support

### Missing Devices
- **ProcessDeviceNotExistsStagingAsync** (line 596): Processes devices that exist in AMOP but missing from API
- **GetTelegenceDeviceBySubscriberNumber**: Validates individual devices via subscriber-level API calls

### Queues
- **SendMessageToGetDevicesQueueAsync** (line 884): Re-enqueues device processing with state management
- **SendMessageToGetDeviceUsageQueueAsync** (line 927): Triggers usage processing Lambda
- **SendMessageToGetDeviceDetailQueueAsync** (line 954): Triggers detail processing Lambda

### Staging Tables
- **TelegenceDeviceStaging**: Main staging table for device data (line 572)
- **TelegenceDeviceBillingNumberAccountStatusStaging**: BAN status staging (line 398)
- **TelegenceDeviceNotExistsStagingToProcess**: Missing devices processing queue

### Stored Procedures
- **usp_Telegence_Truncate_DeviceAndUsageStaging**: Clears device and usage staging data
- **usp_Telegence_Truncate_BillingAccountNumberStatusStaging**: Clears BAN status staging
- **usp_Telegence_Get_CurrentDevicesCount**: Gets existing device count for service provider
- **usp_GetTelegenceDevices_NotExists_Staging**: Identifies missing devices (referenced as "usp_GetTelegenDevice_NotExists_Stagging" in line 802)

### Retry Logic
- **SQL retry**: Uses `PolicyFactory.SqlRetryPolicy()` with `CommonConstants.NUMBER_OF_RETRIES`
- **HTTP retry**: Uses Polly retry policies for API calls
- **Lambda retry**: Uses `RetryNumber` field with configurable limits
- **Exponential backoff**: Implemented through Polly library with increasing delays

## Key Configuration Parameters
- **BatchSize**: 250 (default page size)
- **MaxCyclesToProcess**: Prevents infinite pagination loops
- **NUMBER_OF_TELEGENCE_LAMBDA_RETRIES**: Maximum Lambda retry attempts
- **DELAY_IN_SECONDS_FIVE_SECONDS**: Standard delay between retries
- **REMAINING_TIME_CUT_OFF**: Minimum time required to continue processing

## Processing Flow Summary
1. **Initialization**: Get service provider, clear staging tables
2. **BAN Processing**: Fetch BAN statuses from API, store in staging
3. **Device Processing**: Fetch device list in pages, apply FAN filters, bulk insert
4. **Missing Device Handling**: Validate devices present in AMOP but missing from API
5. **Completion**: Trigger Usage and Detail Lambda functions
6. **Error Handling**: Retry with exponential backoff, re-enqueue on timeout/failure