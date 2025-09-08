using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Models;
using Amazon;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Models.Telegence;
using Amop.Core.Resilience;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxTelegenceAWSGetDevices
{
    public class Function : AwsFunctionBase
    {
        private string TelegenceDevicesGetURL = Environment.GetEnvironmentVariable("TelegenceDevicesGetURL");
        private int MaxCyclesToProcess = Convert.ToInt32(Environment.GetEnvironmentVariable("MaxCyclesToProcess"));
        private string TelegenceDestinationQueueGetDevicesURL = Environment.GetEnvironmentVariable("TelegenceDestinationQueueGetDevicesURL");
        private string DeviceDetailQueueURL = Environment.GetEnvironmentVariable("TelegenceDeviceDetailQueueURL");
        private string TelegenceDeviceDetailGetURL = Environment.GetEnvironmentVariable("TelegenceDeviceDetailGetURL");
        private string TelegenceDeviceUsageQueueURL = Environment.GetEnvironmentVariable("TelegenceDeviceUsageQueueURL");
        private string ProxyUrl = Environment.GetEnvironmentVariable("ProxyUrl");
        private string TelegenceBanDetailGetURL = Environment.GetEnvironmentVariable("TelegenceBanDetailGetURL");
        private int BatchSize = Convert.ToInt32(Environment.GetEnvironmentVariable("BatchSize")); // 250
        private int DEFAULT_BATCH_SIZE = 250;
        private int delayQueue = 60;
        private string CANCEL_STATUS = "C";
        private string SUBSCRIBER_STATUS = "subscriberStatus";

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
        /// to respond to SQS messages.
        /// </summary>
        /// <param name="sqsEvent"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            KeySysLambdaContext keysysContext = null;
            try
            {
                // TEST BUILD WITH NEW DOCKER IMAGE
                keysysContext = BaseFunctionHandler(context);

                if (string.IsNullOrEmpty(TelegenceDevicesGetURL))
                {
                    TelegenceDevicesGetURL = context.ClientContext.Environment["TelegenceDevicesGetURL"];
                    TelegenceDestinationQueueGetDevicesURL = context.ClientContext.Environment["TelegenceDestinationQueueGetDevicesURL"];
                    DeviceDetailQueueURL = context.ClientContext.Environment["TelegenceDeviceDetailQueueURL"];
                    TelegenceDeviceUsageQueueURL = context.ClientContext.Environment["TelegenceDeviceUsageQueueURL"];
                    ProxyUrl = context.ClientContext.Environment["ProxyUrl"];
                    MaxCyclesToProcess = Convert.ToInt32(context.ClientContext.Environment["MaxCyclesToProcess"]);
                    TelegenceBanDetailGetURL = context.ClientContext.Environment["TelegenceBanDetailGetURL"];
                    TelegenceDeviceDetailGetURL = context.ClientContext.Environment["TelegenceDeviceDetailGetURL"];
                    if (context.ClientContext.Environment["BatchSize"].Length > 0)
                    {
                        var batchSize = int.Parse(context.ClientContext.Environment["BatchSize"]);
                        BatchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
                    }
                }

                int processedRecordCount;
                if (sqsEvent?.Records != null)
                {
                    processedRecordCount = sqsEvent.Records.Count;
                    LogInfo(keysysContext, "STATUS", $"TelegenceGetDevices::Beginning to process {processedRecordCount} records...");
                    foreach (var record in sqsEvent.Records)
                    {
                        LogInfo(keysysContext, "MessageId", record.MessageId);
                        LogInfo(keysysContext, "EventSource", record.EventSource);
                        LogInfo(keysysContext, "Body", record.Body);

                        var syncState = new TelegenceGetDevicesSyncState();
                        if (record.MessageAttributes.ContainsKey("CurrentPage"))
                        {
                            syncState.CurrentPage = int.Parse(record.MessageAttributes["CurrentPage"].StringValue);
                            LogInfo(keysysContext, "CurrentPage", syncState.CurrentPage);
                        }

                        if (record.MessageAttributes.ContainsKey("HasMoreData"))
                        {
                            syncState.HasMoreData = record.MessageAttributes["HasMoreData"].StringValue.ToLower() == "true";
                            LogInfo(keysysContext, "HasMoreData", syncState.HasMoreData);
                        }

                        if (record.MessageAttributes.ContainsKey("CurrentServiceProviderId"))
                        {
                            syncState.CurrentServiceProviderId = int.Parse(record.MessageAttributes["CurrentServiceProviderId"].StringValue);
                            LogInfo(keysysContext, "CurrentServiceProviderId", syncState.CurrentServiceProviderId);
                        }
                        // Check initial processing -> true is by default
                        syncState.InitializeProcessing = true;
                        if (record.MessageAttributes.ContainsKey("InitializeProcessing"))
                        {
                            syncState.InitializeProcessing = record.MessageAttributes["InitializeProcessing"].StringValue.ToLower() == "true";
                            LogInfo(keysysContext, "InitializeProcessing", syncState.InitializeProcessing);
                        }

                        syncState.IsProcessDeviceNotExistsStaging = false;
                        if (record.MessageAttributes.ContainsKey("IsProcessDeviceNotExistsStaging"))
                        {
                            syncState.IsProcessDeviceNotExistsStaging = record.MessageAttributes["IsProcessDeviceNotExistsStaging"].StringValue == "1";
                            LogInfo(keysysContext, "IsProcessDeviceNotExistsStaging", syncState.IsProcessDeviceNotExistsStaging);
                        }

                        syncState.IsLastProcessDeviceNotExistsStaging = false;
                        if (record.MessageAttributes.ContainsKey("IsLastProcessDeviceNotExistsStaging"))
                        {
                            syncState.IsLastProcessDeviceNotExistsStaging = record.MessageAttributes["IsLastProcessDeviceNotExistsStaging"].StringValue == "1";
                            LogInfo(keysysContext, "IsLastProcessDeviceNotExistsStaging", syncState.IsLastProcessDeviceNotExistsStaging);
                        }

                        syncState.GroupNumber = 0;
                        if (record.MessageAttributes.ContainsKey("GroupNumber"))
                        {
                            syncState.GroupNumber = int.Parse(record.MessageAttributes["GroupNumber"].StringValue);
                            LogInfo(keysysContext, "GroupNumber", syncState.GroupNumber);
                        }

                        syncState.RetryNumber = 0;
                        if (record.MessageAttributes.ContainsKey("RetryNumber"))
                        {
                            if (int.TryParse(record.MessageAttributes["RetryNumber"].StringValue, out int retryNumber))
                            {
                                syncState.RetryNumber = retryNumber;
                            }
                            LogInfo(keysysContext, "Retry Number", syncState.RetryNumber);
                        }

                        await TryProcessDeviceListAsync(keysysContext, syncState);
                    }
                }
                else
                {
                    var syncState = new TelegenceGetDevicesSyncState
                    {
                        CurrentPage = 1,
                        HasMoreData = true,
                        CurrentServiceProviderId = 0,
                        InitializeProcessing = true,
                        IsProcessDeviceNotExistsStaging = false,
                        GroupNumber = 0,
                        IsLastProcessDeviceNotExistsStaging = false,
                        RetryNumber = 0
                    };
                    processedRecordCount = 1;

                    await TryProcessDeviceListAsync(keysysContext, syncState);
                }

                LogInfo(keysysContext, "STATUS", $"Processed {processedRecordCount} records.");
            }
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

            CleanUp(keysysContext);
        }

        private async Task TryProcessDeviceListAsync(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState)
        {
            try
            {
                var policyFactory = new PolicyFactory(context.logger);
                bool proceed = true;
                if (syncState.CurrentServiceProviderId == 0) /* 0: initial value*/
                {
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
                }

                if (proceed)
                {
                    if (syncState.IsProcessDeviceNotExistsStaging)
                    {
                        var banStatus = GetBanListStatusesStaging(context.CentralDbConnectionString);
                        await ProcessDeviceNotExistsStagingAsync(context, syncState, banStatus, policyFactory);
                        return;
                    }

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
                    else
                    {
                        //Get Billing Account Number Staging Status Database
                        var banStatus = GetBanListStatusesStaging(context.CentralDbConnectionString);
                        var fanFilter = GetFANFilter(context, syncState.CurrentServiceProviderId);
                        if (banStatus.Any())
                        {
                            await ProcessDeviceListAsync(context, syncState, banStatus, fanFilter);
                        }
                        else
                        {
                            // Check if this is possibly first sync of this service provider
                            // By counting existing devices of this service provider
                            await CheckIfServiceProviderFirstSync(context, syncState, policyFactory, banStatus);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogInfo(context, "EXCEPTION:TelegenceGetDevices:", ex.Message + " " + ex.StackTrace);
            }
        }

        protected async Task CheckIfServiceProviderFirstSync(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, PolicyFactory policyFactory, Dictionary<string, string> banStatus)
        {
            var existingDeviceCount = GetTelegenceCurrentDeviceCount(context, syncState.CurrentServiceProviderId, policyFactory);
            var fanFilter = GetFANFilter(context, syncState.CurrentServiceProviderId);

            // If no existing devices, get the device list before getting the BAN status
            if (existingDeviceCount == 0)
            {
                LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.TELEGENCE_FIRST_SYNC_FOR_SERVICE_PROVIDER_MESSAGE, syncState.CurrentServiceProviderId));
                await ProcessDeviceListAsync(context, syncState, banStatus, fanFilter, isFirstSync: true);
            }
            else
            {
                LogInfo(context, CommonConstants.WARNING, string.Format(LogCommonStrings.TELEGENCE_PROVIDER_HAVE_DEVICES_BUT_NO_BAN_MESSAGE, syncState.CurrentServiceProviderId, existingDeviceCount));
            }
        }

        protected int GetTelegenceCurrentDeviceCount(KeySysLambdaContext context, object serviceProviderId, PolicyFactory policyFactory)
        {
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.SERVICE_PROVIDER_ID, serviceProviderId),
            };
            var deviceCount = 0;
            policyFactory.SqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES).Execute(() =>
            {
                deviceCount = SqlQueryHelper.ExecuteStoredProcedureWithIntResult(ParameterizedLog(context),
                context.CentralDbConnectionString,
                SQLConstant.StoredProcedureName.TELEGENCE_GET_CURRENT_DEVICES_COUNT,
                parameters,
                SQLConstant.ShortTimeoutSeconds);
            });
            return deviceCount;
        }

        private async Task<Dictionary<string, string>> ProcessBanListAsync(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, PolicyFactory policyFactory)
        {
            if (syncState.RetryNumber == 0)
            {
                PrepareBanListToProcess(ParameterizedLog(context), context.CentralDbConnectionString, policyFactory);
            }
            List<string> banList = GetBanList(ParameterizedLog(context), context.CentralDbConnectionString, policyFactory);
            if (banList != null)
            {
                var banStatus = new Dictionary<string, string>(banList.Count);

                var telegenceAuth = TelegenceCommon.GetTelegenceAuthenticationInformation(context.CentralDbConnectionString, syncState.CurrentServiceProviderId);

                if (telegenceAuth == null)
                {
                    return new Dictionary<string, string>();
                }

                foreach (var ban in banList)
                {
                    if (context.Context.RemainingTime.TotalSeconds > CommonConstants.REMAINING_TIME_CUT_OFF)
                    {
                        string status = await TelegenceCommon.GetBanStatusAsync(context, telegenceAuth, ProxyUrl, ban, TelegenceBanDetailGetURL);
                        banStatus.Add(ban, status);
                    }
                    else
                    {
                        LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.STOP_GETTING_SINCE_IS_NOT_ENOUGH_TIME, "BAN"));
                        syncState.RetryNumber++;
                        break;
                    }
                }
                MarkProcessForEachBANProcessed(ParameterizedLog(context), context.CentralDbConnectionString, banStatus.Select(x => x.Key).ToList(), policyFactory);
                return banStatus;
            }

            return new Dictionary<string, string>();
        }

        public void PrepareBanListToProcess(Action<string, string> logFunction, string connectionString, PolicyFactory policyFactory)
        {
            try
            {
                policyFactory.SqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES).Execute(() =>
                {
                    SqlQueryHelper.ExecuteStoredProcedureWithRowCountResult(logFunction,
                    connectionString,
                    SQLConstant.StoredProcedureName.USP_TELEGENCE_DEVICES_PREPARE_BANS_TO_PROCESS,
                    new List<SqlParameter>(),
                    SQLConstant.ShortTimeoutSeconds, false, true);
                });
            }
            catch (Exception ex)
            {
                logFunction(CommonConstants.EXCEPTION, String.Format(LogCommonStrings.REQUEST_FAILED_AT_FINAL_RETRIES, SQLConstant.StoredProcedureName.USP_TELEGENCE_DEVICES_PREPARE_BANS_TO_PROCESS, CommonConstants.NUMBER_OF_RETRIES, ex.Message));
            }
        }

        private List<string> GetBanList(Action<string, string> logFunction, string centralDbConnectionString, PolicyFactory policyFactory)
        {
            var banList = new List<string>();
            try
            {
                policyFactory.SqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES).Execute(() =>
                {
                    banList = SqlQueryHelper.ExecuteStoredProcedureWithListResult(logFunction, centralDbConnectionString, SQLConstant.StoredProcedureName.GET_BAN_LIST_NEED_TO_PROCESS, (dataReader) => GetBillingAccountNumberFromDatareader(dataReader), new List<SqlParameter>(), SQLConstant.ShortTimeoutSeconds, true);
                });
            }
            catch (Exception ex)
            {
                logFunction(CommonConstants.EXCEPTION, String.Format(LogCommonStrings.REQUEST_FAILED_AT_FINAL_RETRIES, SQLConstant.StoredProcedureName.GET_BAN_LIST_NEED_TO_PROCESS, CommonConstants.NUMBER_OF_RETRIES, ex.Message));
            }
            return banList;
        }

        private string GetBillingAccountNumberFromDatareader(SqlDataReader dataReader)
        {
            var columns = dataReader.GetColumnsFromReader();
            return dataReader.StringFromReader(columns, CommonColumnNames.BillingAccountNumber);
        }

        private void MarkProcessForEachBANProcessed(Action<string, string> logFunction, string connectionString, List<string> banProcesses, PolicyFactory policyFactory)
        {
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.BILLING_ACCOUNT_NUMBERS, string.Join(",", banProcesses)),
            };
            try
            {
                policyFactory.SqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES).Execute(() =>
                {
                    SqlQueryHelper.ExecuteStoredProcedureWithRowCountResult(logFunction, connectionString, SQLConstant.StoredProcedureName.MARK_PROCESSED_FOR_BAN, parameters, SQLConstant.ShortTimeoutSeconds, false, true);
                });
            }
            catch (Exception ex)
            {
                logFunction(CommonConstants.EXCEPTION, String.Format(LogCommonStrings.REQUEST_FAILED_AT_FINAL_RETRIES, SQLConstant.StoredProcedureName.MARK_PROCESSED_FOR_BAN, CommonConstants.NUMBER_OF_RETRIES, ex.Message));
            }
        }

        private Dictionary<string, string> GetBanListStatusesStaging(string centralDbConnectionString)
        {
            Dictionary<string, string> banStatuses = new Dictionary<string, string>();
            using (SqlConnection connection = new SqlConnection(centralDbConnectionString))
            {
                connection.Open();

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
            }

            return banStatuses;
        }


        private void SaveBillingAccountNumberStatusStaging(KeySysLambdaContext context, Dictionary<string, string> banStatuses, int currentServiceProviderId)
        {
            LogInfo(context, "SUB: SaveBillingAccountNumberStatusStaging", "Start Saving Telegence Billing Account Number Status");
            DataTable table = new DataTable();
            table.Columns.Add("Id");
            table.Columns.Add("BillingAccountNumber");
            table.Columns.Add("Status");
            table.Columns.Add("ServiceProviderId");
            foreach (var banStatus in banStatuses)
            {
                var dr = table.NewRow();
                dr[1] = banStatus.Key;
                dr[2] = banStatus.Value;
                dr[3] = currentServiceProviderId;
                table.Rows.Add(dr);
            }

            SqlBulkCopy(context, context.CentralDbConnectionString, table, "TelegenceDeviceBillingNumberAccountStatusStaging");
            LogInfo(context, "SUB: SaveBillingAccountNumberStatusStaging", "End Saving Telegence Billing Account Number Status");
        }


        protected async Task ProcessDeviceListAsync(
            KeySysLambdaContext context,
            TelegenceGetDevicesSyncState syncState,
            Dictionary<string, string> banStatus,
            Dictionary<string, List<string>> fanFilter = null,
            bool isFirstSync = false)
        {
            LogInfo(context, "CurrentPage", syncState.CurrentPage);
            LogInfo(context, "MaxCyclesToProcess", MaxCyclesToProcess);

            syncState.IsLastCycle = false;
            int cycleCounter = 1;
            var telegenceDeviceList = new List<TelegenceDeviceResponse>();

            // Keeping "MaxCyclesToProcess" in case "isLastCycle" does not get set from API response.  
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

            // First sync so there won't be any existing BANs, we need to get the BANs status from API 
            if (isFirstSync)
            {
                var banList = telegenceDeviceList.Select(x => x.BillingAccountNumber).Distinct();
                if (banList.Any())
                {
                    banStatus = await GetBANStatusFromAPI(context, syncState, banStatus, banList);
                }
            }

            LogInfo(context, nameof(syncState.IsLastCycle), syncState.IsLastCycle);


            if (telegenceDeviceList.Count > 0)
            {
                SaveDevicesToStagingTable(context, syncState, banStatus, telegenceDeviceList, fanFilter);

                // Check to see if there is another service provider to process.
                if (syncState.IsLastCycle)
                {
                    CheckForNextServiceProvider(context, syncState);
                }

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
            }
            else
            {
                //Countinue get Telegence Device Usage If can't get Telegence Device
                await SendMessageToGetDeviceUsageQueueAsync(context, TelegenceDeviceUsageQueueURL, 5);
            }
        }

        protected virtual void CheckForNextServiceProvider(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState)
        {
            syncState.HasMoreData = false;
            var nextServiceProvider = ServiceProviderCommon.GetNextServiceProviderId(context.CentralDbConnectionString, Amop.Core.Models.IntegrationType.Telegence, syncState.CurrentServiceProviderId);
            if (nextServiceProvider > 0)
            {
                syncState.CurrentServiceProviderId = nextServiceProvider;
            }
        }

        // Separated function for testing purpose
        protected virtual async Task<TelegenceGetDevicesSyncState> GetTelegenceDevicesFromAPI(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, List<TelegenceDeviceResponse> telegenceDeviceList)
        {
            LogInfo(context, CommonConstants.SUB, $"{nameof(telegenceDeviceList)} : {telegenceDeviceList.Count}");
            return await TelegenceCommon.GetTelegenceDevicesAsync(context, syncState, ProxyUrl, telegenceDeviceList, TelegenceDevicesGetURL, BatchSize);
        }

        protected virtual void SaveDevicesToStagingTable(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, Dictionary<string, string> banStatus, List<TelegenceDeviceResponse> telegenceDeviceList, Dictionary<string, List<string>> fanFilter = null)
        {
            LogInfo(context, "SaveDevicesToStagingTable", $"Group number: {syncState.GroupNumber}");
            var includedFANs = fanFilter != null && fanFilter.ContainsKey("IncludedFANs") ? fanFilter["IncludedFANs"] : new List<string>();
            var excludedFANs = fanFilter != null && fanFilter.ContainsKey("ExcludedFANs") ? fanFilter["ExcludedFANs"] : new List<string>();
            var filteredTelegenceDeviceList = telegenceDeviceList.ToList();

            if (includedFANs.Count > 0)
            {
                filteredTelegenceDeviceList = filteredTelegenceDeviceList.Where(x => includedFANs.Contains(x.FoundationAccountNumber)).ToList();
            }
            if (excludedFANs.Count > 0)
            {
                filteredTelegenceDeviceList = filteredTelegenceDeviceList.Where(x => !excludedFANs.Contains(x.FoundationAccountNumber)).ToList();
            }

            DataTable table = new DataTable();
            table.Columns.Add(CommonColumnNames.Id);
            table.Columns.Add(CommonColumnNames.ServiceProviderId);
            table.Columns.Add(CommonColumnNames.FoundationAccountNumber);
            table.Columns.Add(CommonColumnNames.BillingAccountNumber);
            table.Columns.Add(CommonColumnNames.SubscriberNumber);
            table.Columns.Add(CommonColumnNames.SubscriberNumberStatus);
            table.Columns.Add(CommonColumnNames.RefreshTimestamp);
            table.Columns.Add(CommonColumnNames.CreatedDate);
            table.Columns.Add(CommonColumnNames.BanStatus);

            foreach (var telegenceDevice in filteredTelegenceDeviceList)
            {
                var banStatusText = GetBanStatusTextForDevice(banStatus, telegenceDevice);
                var dr = AddToDataRow(context, table, telegenceDevice, banStatusText, syncState.CurrentServiceProviderId);
                table.Rows.Add(dr);
            }
            LogInfo(context, CommonConstants.STATUS, LogCommonStrings.SQL_BULK_COPY_START);
            SqlBulkCopy(context, context.CentralDbConnectionString, table, DatabaseTableNames.TELEGENCE_DEVICE_STAGING);
        }

        protected virtual async Task<Dictionary<string, string>> GetBANStatusFromAPI(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, Dictionary<string, string> banStatus, IEnumerable<string> banList)
        {
            var telegenceAuth = TelegenceCommon.GetTelegenceAuthenticationInformation(context.CentralDbConnectionString, syncState.CurrentServiceProviderId);

            if (telegenceAuth != null)
            {
                foreach (var ban in banList)
                {
                    string status = await TelegenceCommon.GetBanStatusAsync(context, telegenceAuth, ProxyUrl, ban, TelegenceBanDetailGetURL);
                    banStatus.Add(ban, status);
                }
            }
            else
            {
                banStatus = new Dictionary<string, string>();
                LogInfo(context, CommonConstants.ERROR, string.Format(LogCommonStrings.FAILED_GET_AUTHENTICATION_INFORMATION, CommonConstants.TELEGENCE_CARRIER_NAME));
            }

            return banStatus;
        }

        private async Task ProcessDeviceNotExistsStagingAsync(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, Dictionary<string, string> banStatus, PolicyFactory policyFactory)
        {
            LogInfo(context, "ProcessDeviceNotExistsStagingAsync", $"Group number: {syncState.GroupNumber}");


            List<TelegenceDeviceResponse> telegenceDevicesFromProcess = GetTelegenceDeviceNotExistsOnStagingToProcess(ParameterizedLog(context), context.CentralDbConnectionString, syncState, policyFactory);

            DataTable table = new DataTable();
            table.Columns.Add("Id");
            table.Columns.Add("ServiceProviderId");
            table.Columns.Add("FoundationAccountNumber");
            table.Columns.Add("BillingAccountNumber");
            table.Columns.Add("SubscriberNumber");
            table.Columns.Add("SubscriberNumberStatus");
            table.Columns.Add("RefreshTimestamp");
            table.Columns.Add("CreatedDate");
            table.Columns.Add("BanStatus");

            LogInfo(context, "Device Need Check API", $"Has {telegenceDevicesFromProcess.Count} Need Check API.");

            var listDevicesProcessed = new List<string>();
            foreach (var telegenceDevice in telegenceDevicesFromProcess)
            {
                if (context.Context.RemainingTime.TotalSeconds > CommonConstants.REMAINING_TIME_CUT_OFF)
                {
                    //get device detail
                    var telegenceAuthenticationInfo = TelegenceCommon.GetTelegenceAuthenticationInformation(context.CentralDbConnectionString, syncState.CurrentServiceProviderId);
                    if (telegenceAuthenticationInfo == null)
                    {
                        throw new Exception($"Error Getting Info: Failed to get Telegence Authentication Information.");
                    }
                    string resultAPI = await TelegenceCommon.GetTelegenceDeviceBySubscriberNumber(context, telegenceAuthenticationInfo, context.IsProduction,
                                telegenceDevice.SubscriberNumber, TelegenceDeviceDetailGetURL, ProxyUrl);

                    if (!string.IsNullOrWhiteSpace(resultAPI))
                    {
                        var deviceDetail = JsonConvert.DeserializeObject<TelegenceMobilityLineConfigurationResponse>(resultAPI);
                        var subscriberStatus = deviceDetail.serviceCharacteristic.Where(x => x.Name == SUBSCRIBER_STATUS).Select(x => x.Value).FirstOrDefault();
                        if (!string.IsNullOrEmpty(subscriberStatus))
                        {
                            // if device status is "C" => ignore, b/cDevice List Api not return status "C"
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

                    listDevicesProcessed.Add(telegenceDevice.SubscriberNumber);
                }
                else
                {
                    LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.STOP_GETTING_SINCE_IS_NOT_ENOUGH_TIME, "Telegence Device Details"));
                    syncState.RetryNumber++;
                    break;
                }
            }
            if (table.Rows.Count > 0)
            {
                LogInfo(context, "STATUS", "Insert SIMs exists in AMOP not exists in Device List From Api.");
                SqlBulkCopy(context, context.CentralDbConnectionString, table, "TelegenceDeviceStaging");
            }

            if (listDevicesProcessed.Count > 0)
            {
                MarkProcessForEachDevicesHaveProcessed(ParameterizedLog(context), context.CentralDbConnectionString, listDevicesProcessed, policyFactory);
            }

            var remainingDevicesNeedToProcesses = GetTelegenceDeviceNotExistsOnStagingToProcess(ParameterizedLog(context), context.CentralDbConnectionString, syncState, policyFactory);

            if (remainingDevicesNeedToProcesses.Count > 0 && syncState.RetryNumber <= CommonConstants.NUMBER_OF_TELEGENCE_LAMBDA_RETRIES)
            {
                var isLastProcessDeviceNotExistsStaging = 0;
                if (syncState.IsLastProcessDeviceNotExistsStaging)
                {
                    isLastProcessDeviceNotExistsStaging = 1;
                }
                await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS, syncState.GroupNumber, 1, isLastProcessDeviceNotExistsStaging);
            }
            else
            {
                LogInfo(context, "INFO", $"END ProcessDeviceNotExistsStagingAsync: has {table.Rows.Count} added TelegenceDeviceStaging.");

                if (syncState.IsLastProcessDeviceNotExistsStaging)
                {
                    LogInfo(context, "INFO", $"END ProcessDeviceNotExistsStagingAsync --> Run Process Get Usage And Get Detail.");

                    await SendMessageToGetDeviceUsageQueueAsync(context, TelegenceDeviceUsageQueueURL, CommonConstants.DELAY_IN_SECONDS_FIVE_SECONDS);
                    await SendMessageToGetDeviceDetailQueueAsync(context, DeviceDetailQueueURL, delayQueue); // give usage time to process before starting detail processing
                }
            }
        }

        private List<TelegenceDeviceResponse> GetTelegenceDeviceNotExistsOnStagingToProcess(Action<string, string> logFunction, string centralDbConnectionString, TelegenceGetDevicesSyncState syncState, PolicyFactory policyFactory)
        {
            var telegenceDevices = new List<TelegenceDeviceResponse>();
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.GROUP_NUMBER, syncState.GroupNumber),
            };
            try
            {
                policyFactory.SqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES).Execute(() =>
                {
                    telegenceDevices = SqlQueryHelper.ExecuteStoredProcedureWithListResult(logFunction, centralDbConnectionString, SQLConstant.StoredProcedureName.GET_TELEGENCE_DEVICES_NOT_EXISTS_ON_STAGING, (dataReader) => TelegenceDeviceResponseFromReader(dataReader), parameters, SQLConstant.ShortTimeoutSeconds, true);
                });
            }
            catch (Exception ex)
            {
                logFunction(CommonConstants.EXCEPTION, String.Format(LogCommonStrings.REQUEST_FAILED_AT_FINAL_RETRIES, SQLConstant.StoredProcedureName.GET_TELEGENCE_DEVICES_NOT_EXISTS_ON_STAGING, CommonConstants.NUMBER_OF_RETRIES, ex.Message));
            }
            return telegenceDevices;
        }

        private void MarkProcessForEachDevicesHaveProcessed(Action<string, string> logFunction, string connectionString, List<string> subscriberNumbers, PolicyFactory policyFactory)
        {
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.SUBSCRIBER_NUMBERS, string.Join(",", subscriberNumbers)),
            };
            try
            {
                policyFactory.SqlRetryPolicy(CommonConstants.NUMBER_OF_RETRIES).Execute(() =>
                {
                    SqlQueryHelper.ExecuteStoredProcedureWithRowCountResult(logFunction, connectionString, SQLConstant.StoredProcedureName.MARK_TELEGENCE_DEVICES_PROCESSED_ON_PROCESS_CHECK_EXISTS_ON_STAGING, parameters);
                });
            }
            catch (Exception ex)
            {
                logFunction(CommonConstants.EXCEPTION, String.Format(LogCommonStrings.REQUEST_FAILED_AT_FINAL_RETRIES, SQLConstant.StoredProcedureName.MARK_TELEGENCE_DEVICES_PROCESSED_ON_PROCESS_CHECK_EXISTS_ON_STAGING, CommonConstants.NUMBER_OF_RETRIES, ex.Message));
            }
        }

        private TelegenceDeviceResponse TelegenceDeviceResponseFromReader(SqlDataReader dataReader)
        {
            var columns = dataReader.GetColumnsFromReader();
            return new TelegenceDeviceResponse()
            {
                SubscriberNumber = dataReader.StringFromReader(columns, CommonColumnNames.SubscriberNumber),
                FoundationAccountNumber = dataReader.StringFromReader(columns, CommonColumnNames.FoundationAccountNumber),
                BillingAccountNumber = dataReader.StringFromReader(columns, CommonColumnNames.BillingAccountNumber),
                SubscriberNumberStatus = dataReader.StringFromReader(columns, CommonColumnNames.SubscriberNumberStatus),
            };
        }

        protected virtual async Task SendProcessDeviceNotExistsStagingMessagesToQueueAsync(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, int groupCount)
        {
            LogInfo(context, "INFO", "End execute RemovePreviouslyStagedFeatures");
            var delayQueue = 5;
            var isLastGroup = 0;

            // if iGroup is last, delay 60s
            for (int iGroup = 0; iGroup <= groupCount; iGroup++)
            {
                if (iGroup == groupCount)
                {
                    delayQueue = 60;
                    isLastGroup = 1;
                }
                await SendMessageToGetDevicesQueueAsync(context, syncState, TelegenceDestinationQueueGetDevicesURL, delayQueue, iGroup, 1, isLastGroup);
            }
        }

        protected virtual int GetGroupCount(KeySysLambdaContext context)
        {
            LogInfo(context, "INFO", $"GetGroupCount tableName: TelegenceDeviceNotExistsStagingToProcess");

            int groupCount = 0;
            using (var con = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand($"SELECT MAX(GroupNumber) FROM [dbo].[TelegenceDeviceNotExistsStagingToProcess]", con)
                {
                    CommandType = CommandType.Text
                })
                {
                    con.Open();
                    var scalarResult = cmd.ExecuteScalar();
                    if (scalarResult != null && scalarResult != DBNull.Value)
                    {
                        groupCount = (int)scalarResult;
                    }
                }
                // Close destination connection
                con.Close();
            }

            LogInfo(context, "Group Count", groupCount);
            return groupCount;
        }

        protected virtual void GetTelegenceDeviceNotExistsStaging(KeySysLambdaContext context)
        {
            LogInfo(context, "INFO", $"GetTelegenDeviceNotExistsStagging");

            using (var con = new SqlConnection(context.CentralDbConnectionString))
            {
                string cmdText = $"usp_GetTelegenDevice_NotExists_Stagging";
                using (var cmd = new SqlCommand(cmdText, con)
                {
                    CommandType = CommandType.StoredProcedure
                })
                {
                    cmd.CommandTimeout = 800;
                    con.Open();
                    cmd.Parameters.AddWithValue("@BatchSize", BatchSize);
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private string GetBanStatusTextForDevice(Dictionary<string, string> banStatus, TelegenceDeviceResponse telegenceDevice)
        {
            string ban = telegenceDevice.BillingAccountNumber;
            if (!string.IsNullOrEmpty(ban) && banStatus.Count > 0 && banStatus.ContainsKey(ban))
            {
                return banStatus[ban];
            }

            return null;
        }

        private DataRow AddToDataRow(KeySysLambdaContext context, DataTable table, TelegenceDeviceResponse device, string banStatusText, int currentServiceProviderId)
        {
            var dr = table.NewRow();
            dr[1] = currentServiceProviderId;
            dr[2] = device.FoundationAccountNumber;
            dr[3] = device.BillingAccountNumber;
            dr[4] = device.SubscriberNumber;
            dr[6] = device.RefreshTimestamp;
            dr[7] = DateTime.UtcNow;
            dr[8] = banStatusText;
            dr[5] = device.SubscriberNumberStatus;

            return dr;
        }

        private DataRow AddToDeviceNotExistsStagingRow(KeySysLambdaContext context, DataTable table, TelegenceDeviceResponse device, int currentServiceProviderId, int groupNumber)
        {
            var dr = table.NewRow();
            dr[1] = device.SubscriberNumber;
            dr[2] = currentServiceProviderId;
            dr[3] = groupNumber;

            return dr;
        }

        private void TruncateTelegenceDeviceAndUsageStaging(KeySysLambdaContext context)
        {
            using (var Conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var Cmd = new SqlCommand("usp_Telegence_Truncate_DeviceAndUsageStaging", Conn))
                {
                    Cmd.CommandType = CommandType.StoredProcedure;
                    Cmd.CommandTimeout = 9000;
                    Conn.Open();

                    Cmd.ExecuteNonQuery();
                    Conn.Close();
                }
            }
        }

        private void TruncateTelegenceBillingAccountNumberStatusStaging(KeySysLambdaContext context)
        {
            using (var Conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var Cmd = new SqlCommand("usp_Telegence_Truncate_BillingAccountNumberStatusStaging", Conn))
                {
                    Cmd.CommandType = CommandType.StoredProcedure;
                    Cmd.CommandTimeout = 9000;
                    Conn.Open();

                    Cmd.ExecuteNonQuery();
                    Conn.Close();
                }
            }
        }

        protected virtual async Task SendMessageToGetDevicesQueueAsync(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState,
            string telegenceDestinationQueueGetDevicesURL, int delaySeconds, int groupNumber = 0, int isProcessDeviceNotExistsStaging = 0, int isLastGroup = 0)
        {
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

            using (var client = new AmazonSQSClient(AwsCredentials(context), RegionEndpoint.USEast1))
            {
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

                LogInfo(context, "MessageBody", request.MessageBody);

                var response = await client.SendMessageAsync(request);
                LogInfo(context, "RESPONSE STATUS", $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
            }
        }

        protected virtual async Task SendMessageToGetDeviceUsageQueueAsync(KeySysLambdaContext context, string deviceUsageQueueURL, int delaySeconds)
        {
            LogInfo(context, "SUB", "SendMessageToGetDeviceUsageQueueAsync");
            LogInfo(context, "InitializeProcessing", true);
            LogInfo(context, "DeviceUsageQueueURL", deviceUsageQueueURL);
            LogInfo(context, "DelaySeconds", delaySeconds);

            using (var client = new AmazonSQSClient(AwsCredentials(context), RegionEndpoint.USEast1))
            {
                var request = new SendMessageRequest
                {
                    DelaySeconds = delaySeconds,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {"InitializeProcessing", new MessageAttributeValue {DataType = "String", StringValue = true.ToString()}}
                    },
                    MessageBody = "Start processing Telegence device usage",
                    QueueUrl = deviceUsageQueueURL
                };

                LogInfo(context, "MessageBody", request.MessageBody);

                var response = await client.SendMessageAsync(request);
                LogInfo(context, "RESPONSE STATUS", $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
            }
        }

        private async Task SendMessageToGetDeviceDetailQueueAsync(KeySysLambdaContext context, string deviceDetailQueueURL, int delaySeconds)
        {
            LogInfo(context, "SUB", "SendMessageToGetDeviceDetailQueueAsync");
            LogInfo(context, "InitializeProcessing", true);
            LogInfo(context, "DeviceDetailQueueURL", deviceDetailQueueURL);
            LogInfo(context, "DelaySeconds", delaySeconds);

            using (var client = new AmazonSQSClient(AwsCredentials(context), RegionEndpoint.USEast1))
            {
                var request = new SendMessageRequest
                {
                    DelaySeconds = delaySeconds,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {"InitializeProcessing", new MessageAttributeValue {DataType = "String", StringValue = true.ToString()}},
                        {"GroupNumber", new MessageAttributeValue {DataType = "String", StringValue = 0.ToString()}}
                    },
                    MessageBody = "Start processing Telegence device details",
                    QueueUrl = deviceDetailQueueURL
                };

                LogInfo(context, "MessageBody", request.MessageBody);

                var response = await client.SendMessageAsync(request);
                LogInfo(context, "RESPONSE STATUS", $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
            }
        }
    }
}
