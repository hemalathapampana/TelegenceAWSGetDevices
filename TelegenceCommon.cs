using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Altaworx.AWS.Core.Helpers;
using Altaworx.AWS.Core.Helpers.Constants;
using Altaworx.AWS.Core.Models;
using Amop.Core.Constants;
using Amop.Core.Logger;
using Amop.Core.Models.Telegence;
using Amop.Core.Models.Telegence.Api;
using Amop.Core.Services.Base64Service;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;

namespace Altaworx.AWS.Core
{
    public static class TelegenceCommon
    {
        public static TelegenceAuthentication GetTelegenceAuthenticationInformation(string connectionString, int serviceProviderId)
        {
            TelegenceAuthentication telegenceAuthentication = null;
            try
            {
                using (var Conn = new SqlConnection(connectionString))
                {
                    using (var Cmd = new SqlCommand("usp_Telegence_Get_AuthenticationByProviderId", Conn))
                    {
                        Cmd.CommandType = CommandType.StoredProcedure;
                        Cmd.Parameters.AddWithValue("@providerId", serviceProviderId);
                        Conn.Open();

                        SqlDataReader rdr = Cmd.ExecuteReader();
                        while (rdr.Read())
                        {
                            int billPeriodEndDayIndex = rdr.GetOrdinal("BillPeriodEndDay");
                            telegenceAuthentication = new TelegenceAuthentication()
                            {
                                TelegenceAuthenticationId = Convert.ToInt32(rdr["integrationAuthenticationId"]),
                                ProductionUrl = rdr["productionUrl"].ToString(),
                                SandboxUrl = rdr["sandboxUrl"].ToString(),
                                ClientId = rdr["ClientId"].ToString(),
                                ClientSecret = rdr["ClientSecrect"].ToString(),
                                WriteIsEnabled = rdr.GetBoolean(rdr.GetOrdinal("WriteIsEnabled")),
                                BillPeriodEndDay = rdr.IsDBNull(billPeriodEndDayIndex) ? 1 : rdr.GetInt32(billPeriodEndDayIndex),
                                Password = rdr["password"].ToString(),
                                UserName = rdr["username"].ToString(),
                            };
                            break;
                        }

                        Conn.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.Message);
            }

            return telegenceAuthentication;
        }

        public static List<TelegenceBillingAccount> GetTelegenceBillingAccounts(string connectionString, int serviceProviderId)
        {
            List<TelegenceBillingAccount> billingAccounts = new List<TelegenceBillingAccount>();
            try
            {
                using (var Conn = new SqlConnection(connectionString))
                {
                    using (var Cmd = new SqlCommand("usp_Telegence_Get_BillingAccountsByProviderId", Conn))
                    {
                        Cmd.CommandType = CommandType.StoredProcedure;
                        Cmd.Parameters.AddWithValue("@providerId", serviceProviderId);
                        Conn.Open();

                        SqlDataReader rdr = Cmd.ExecuteReader();
                        while (rdr.Read())
                        {
                            var billingAccount = new TelegenceBillingAccount()
                            {
                                FoundationAccountNumber = rdr["FoundationAccountNumber"].ToString(),
                                BillingAccountNumber = rdr["BillingAccountNumber"].ToString()
                            };

                            billingAccounts.Add(billingAccount);
                        }
                        Conn.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.Message);
            }

            return billingAccounts;
        }

        public static bool UpdateTelegenceDeviceStatus(IKeysysLogger logger, Base64Service base64Service, TelegenceAuthentication telegenceAuthentication, bool isProduction, List<TelegenceActivationRequest> request, string endpoint, string proxyUrl)
        {
            if (telegenceAuthentication.WriteIsEnabled)
            {
                using (var client = new HttpClient())
                {
                    Uri baseUrl = new Uri(telegenceAuthentication.SandboxUrl);
                    if (isProduction)
                    {
                        baseUrl = new Uri(telegenceAuthentication.ProductionUrl);
                    }

                    if (!string.IsNullOrWhiteSpace(proxyUrl))
                    {
                        var headerContent = new ExpandoObject() as IDictionary<string, object>;
                        headerContent.Add("app-id", telegenceAuthentication.ClientId);
                        headerContent.Add("app-secret", telegenceAuthentication.ClientSecret);
                        var headerContentString = JsonConvert.SerializeObject(headerContent);
                        var jsonContentString = JsonConvert.SerializeObject(request);

                        var payload = new PayloadModel()
                        {
                            AuthenticationType = AuthenticationType.TELEGENCEAUTH,
                            Endpoint = endpoint,
                            HeaderContent = headerContentString,
                            JsonContent = jsonContentString,
                            Password = null,
                            Token = null,
                            Url = baseUrl.ToString(),
                            Username = null
                        };

                        var result = client.PostWithProxy(proxyUrl, payload, logger);
                        if (result.IsSuccessful)
                        {
                            return true;
                        }
                        else
                        {
                            string responseBody = result.ResponseMessage;
                            logger.LogInfo("UpdateTelegenceDeviceStatus", $"Proxy call to {endpoint} failed.");
                            logger.LogInfo("Response Error", responseBody);
                            return false;
                        }
                    }
                    else
                    {
                        client.BaseAddress = baseUrl;
                        client.DefaultRequestHeaders.Add("app-id", telegenceAuthentication.ClientId);
                        client.DefaultRequestHeaders.Add("app-secret", telegenceAuthentication.ClientSecret);

                        var payloadAsJson = JsonConvert.SerializeObject(request);
                        var content = new StringContent(payloadAsJson, Encoding.UTF8, "application/json");

                        try
                        {
                            var response = client.PostAsync(endpoint, content).Result;
                            if (response.IsSuccessStatusCode)
                            {
                                return true;
                            }
                            else
                            {
                                string responseBody = response.Content.ReadAsStringAsync().Result;
                                logger.LogInfo("UpdateTelegenceDeviceStatus", $"Call to {endpoint} failed.");
                                logger.LogInfo("EXCEPTION", responseBody);
                                return false;
                            }
                        }
                        catch (Exception e)
                        {
                            logger.LogInfo("UpdateTelegenceDeviceStatus", $"Call to {endpoint} failed.");
                            logger.LogInfo("EXCEPTION", e.Message);
                            return false;
                        }
                    }
                }
            }
            else
            {
                logger.LogInfo("WARN", "Writes disabled for this service provider.");
                return false;
            }
        }

        public static bool UpdateTelegenceSubscriber(IKeysysLogger logger, Base64Service base64Service, TelegenceAuthentication telegenceAuthentication, bool isProduction, TelegenceSubscriberUpdateRequest request, string subscriberNo, string endpoint, string proxyUrl)
        {
            if (telegenceAuthentication.WriteIsEnabled)
            {
                var subscriberUpdateURL = endpoint + subscriberNo;

                using (var client = new HttpClient())
                {
                    Uri baseUrl = new Uri(telegenceAuthentication.SandboxUrl);
                    if (isProduction)
                    {
                        baseUrl = new Uri(telegenceAuthentication.ProductionUrl);
                    }

                    if (!string.IsNullOrWhiteSpace(proxyUrl))
                    {
                        var headerContent = new ExpandoObject() as IDictionary<string, object>;
                        headerContent.Add("app-id", telegenceAuthentication.ClientId);
                        headerContent.Add("app-secret", telegenceAuthentication.ClientSecret);
                        var headerContentString = JsonConvert.SerializeObject(headerContent);
                        var jsonContentString = JsonConvert.SerializeObject(request, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });

                        var payload = new PayloadModel()
                        {
                            AuthenticationType = AuthenticationType.TELEGENCEAUTH,
                            Endpoint = subscriberUpdateURL,
                            HeaderContent = headerContentString,
                            JsonContent = jsonContentString,
                            Password = null,
                            Token = null,
                            Url = baseUrl.ToString(),
                            Username = null
                        };

                        var result = client.PatchWithProxy(proxyUrl, payload, logger);
                        if (result.IsSuccessful)
                        {
                            return true;
                        }
                        else
                        {
                            string responseBody = result.ResponseMessage;
                            logger.LogInfo("UpdateTelegenceSubscriber", $"Proxy call to {endpoint} failed.");
                            logger.LogInfo("Response Error", responseBody);
                            return false;
                        }
                    }
                    else
                    {
                        client.BaseAddress = new Uri(baseUrl + subscriberUpdateURL);
                        client.DefaultRequestHeaders.Add("app-id", telegenceAuthentication.ClientId);
                        client.DefaultRequestHeaders.Add("app-secret", telegenceAuthentication.ClientSecret);

                        var payloadAsJson = JsonConvert.SerializeObject(request);
                        var content = new StringContent(payloadAsJson, Encoding.UTF8, "application/json");

                        try
                        {
                            var response = client.Patch(client.BaseAddress, content);
                            if (response.IsSuccessStatusCode)
                            {
                                return true;
                            }
                            else
                            {
                                return false;
                            }
                        }
                        catch (Exception e)
                        {
                            logger.LogInfo("UpdateTelegenceSubscriber", $"Call to {endpoint} failed.");
                            logger.LogInfo("EXCEPTION", e.Message);
                            return false;
                        }
                    }
                }
            }
            else
            {
                logger.LogInfo("WARN", "Writes disabled for this service provider.");
                return false;
            }
        }

        public static bool UpdateTelegenceMobilityConfiguration(IKeysysLogger logger, Base64Service base64Service, TelegenceAuthentication telegenceAuthentication, bool isProduction, TelegenceMobilityConfigurationRequest request, string subscriberNo, string endpoint, string proxyUrl)
        {
            if (telegenceAuthentication.WriteIsEnabled)
            {
                var mobilityConfigurationURL = endpoint + subscriberNo;

                using (var client = new HttpClient())
                {
                    Uri baseUrl = new Uri(telegenceAuthentication.SandboxUrl);
                    if (isProduction)
                    {
                        baseUrl = new Uri(telegenceAuthentication.ProductionUrl);
                    }

                    if (!string.IsNullOrWhiteSpace(proxyUrl))
                    {
                        var headerContent = new ExpandoObject() as IDictionary<string, object>;
                        headerContent.Add("app-id", telegenceAuthentication.ClientId);
                        headerContent.Add("app-secret", telegenceAuthentication.ClientSecret);
                        var headerContentString = JsonConvert.SerializeObject(headerContent);
                        var jsonContentString = JsonConvert.SerializeObject(request, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });

                        var payload = new PayloadModel()
                        {
                            AuthenticationType = AuthenticationType.TELEGENCEAUTH,
                            Endpoint = mobilityConfigurationURL,
                            HeaderContent = headerContentString,
                            JsonContent = jsonContentString,
                            Password = null,
                            Token = null,
                            Url = baseUrl.ToString(),
                            Username = null
                        };

                        var result = client.PatchWithProxy(proxyUrl, payload, logger);
                        if (result.IsSuccessful)
                        {
                            return true;
                        }
                        else
                        {
                            string responseBody = result.ResponseMessage;
                            logger.LogInfo("UpdateTelegenceMobilityConfiguration", $"Proxy call to {endpoint} failed.");
                            logger.LogInfo("Response Error", responseBody);
                            return false;
                        }
                    }
                    else
                    {
                        client.BaseAddress = new Uri(baseUrl + mobilityConfigurationURL);
                        client.DefaultRequestHeaders.Add("app-id", telegenceAuthentication.ClientId);
                        client.DefaultRequestHeaders.Add("app-secret", telegenceAuthentication.ClientSecret);

                        var payloadAsJson = JsonConvert.SerializeObject(request);
                        var content = new StringContent(payloadAsJson, Encoding.UTF8, "application/json");

                        try
                        {
                            var response = client.Patch(client.BaseAddress, content);
                            if (response.IsSuccessStatusCode)
                            {
                                return true;
                            }
                            else
                            {
                                return false;
                            }
                        }
                        catch (Exception e)
                        {
                            logger.LogInfo("UpdateTelegenceMobilityConfiguration", $"Call to {endpoint} failed.");
                            logger.LogInfo("EXCEPTION", e.Message);
                            return false;
                        }
                    }
                }
            }
            else
            {
                logger.LogInfo("WARN", "Writes disabled for this service provider.");
                return false;
            }
        }

        public static async Task<string> GetTelegenceDeviceBySubscriberNumber(KeySysLambdaContext context, TelegenceAuthentication telegenceAuthentication,
            bool isProduction, string subscriberNo, string endpoint, string proxyUrl)
        {
            AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.GET_DEVICE_DETAIL_BY_SUBCRIBER_NUMBER, subscriberNo));
            var deviceDetailEndpoint = $"{endpoint}{subscriberNo}";
            AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, $"endpoint: {deviceDetailEndpoint}");

            Uri baseUrl = new Uri(telegenceAuthentication.SandboxUrl);
            if (isProduction)
            {
                baseUrl = new Uri(telegenceAuthentication.ProductionUrl);
            }

            if (!string.IsNullOrWhiteSpace(proxyUrl))
            {
                return await GetTelegenceDeviceBySubscriberNumberByProxy(context, telegenceAuthentication, deviceDetailEndpoint, proxyUrl, baseUrl.ToString());
            }
            else
            {
                var deviceDetailUrl = new Uri(baseUrl, deviceDetailEndpoint);
                return await GetTelegenceDeviceBySubscriberNumberWithoutProxy(context, telegenceAuthentication, deviceDetailUrl.AbsoluteUri);
            }
        }

        public static async Task<string> TelegenceGetDetailDataUsage(IKeysysLogger logger, IBase64Service base64Service, TelegenceAuthentication telegenceAuthentication,
            bool isProduction, string subscriberNo, string endpoint, string proxyUrl)
        {
            if (telegenceAuthentication.WriteIsEnabled)
            {
                using (var client = new HttpClient())
                {
                    Uri baseUrl = new Uri(telegenceAuthentication.SandboxUrl);
                    if (isProduction)
                    {
                        baseUrl = new Uri(telegenceAuthentication.ProductionUrl);
                    }

                    HttpRequestMessage request = new HttpRequestMessage
                    {
                        RequestUri = new Uri($"{baseUrl}{endpoint}"),
                        Headers =
                         {
                             {"app-secret", telegenceAuthentication.ClientSecret},
                             {"app-id", telegenceAuthentication.ClientId},
                             {"subscriber", subscriberNo },
                         },
                        Method = HttpMethod.Get,

                    };
                    request.Content = new StringContent("");

                    try
                    {
                        var response = await client.SendAsync(request);
                        var content = await response.Content.ReadAsStringAsync();
                        if (response.IsSuccessStatusCode)
                        {
                            return content;
                        }
                        else
                        {
                            return null;
                        }
                    }
                    catch (Exception e)
                    {
                        logger.LogInfo("TelegenceGetDetailDataUsage", $"Call to {endpoint} failed.");
                        logger.LogInfo("EXCEPTION", e.Message);
                        return null;

                    }
                }
            }
            else
            {
                logger.LogInfo("WARN", "Writes disabled for this service provider.");
                return null;
            }
        }

        public static async Task<TelegenceGetDevicesSyncState> GetTelegenceDevicesAsync(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, string proxyUrl,
           List<TelegenceDeviceResponse> telegenceDeviceList, string deviceDetailEndpoint, int pageSize)
        {
            AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, $"GetTelegenceDevicesAsync: {proxyUrl}, {syncState.CurrentPage}");

            var telegenceAuth = GetTelegenceAuthenticationInformation(context.CentralDbConnectionString, syncState.CurrentServiceProviderId);

            if (telegenceAuth == null)
            {
                syncState.IsLastCycle = true;
                return syncState;
            }

            AwsFunctionBase.LogInfo(context, "GetTelegenceDevicesAsync::TelegenceAPIClientId", telegenceAuth.ClientId);
            AwsFunctionBase.LogInfo(context, "GetTelegenceDevicesAsync::ProxyUrl", proxyUrl);
            Uri baseUrl = new Uri(telegenceAuth.SandboxUrl);
            if (context.IsProduction)
            {
                baseUrl = new Uri(telegenceAuth.ProductionUrl);
            }

            if (!string.IsNullOrWhiteSpace(proxyUrl))
            {
                syncState = await GetTelegenceDevicesAsyncByProxy(context, syncState, telegenceAuth, proxyUrl, telegenceDeviceList, pageSize, deviceDetailEndpoint, baseUrl.ToString());
            }
            else
            {
                var deviceDetailRequestUrl = $"{baseUrl.AbsoluteUri.TrimEnd('/')}{deviceDetailEndpoint}";
                syncState = await GetTelegenceDevicesAsyncWithoutProxy(context, telegenceAuth, syncState, telegenceDeviceList, deviceDetailRequestUrl, pageSize);
            }

            return syncState;
        }

        public static async Task<string> GetBanStatusAsync(KeySysLambdaContext context, TelegenceAuthentication telegenceAuth, string proxyUrl, string ban, string telegenceBanDetailGetURL)
        {
            AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.GET_BAN_STATUS_BY_BAN_NUMBER, ban));
            var status = string.Empty;
            string banDetailUrl = telegenceBanDetailGetURL.Replace("{ban}", ban);
            Uri baseUrl = new Uri(telegenceAuth.SandboxUrl);
            if (context.IsProduction)
            {
                baseUrl = new Uri(telegenceAuth.ProductionUrl);
            }

            if (!string.IsNullOrWhiteSpace(proxyUrl))
            {
                status = await GetBanStatusAsyncByProxy(context, telegenceAuth, banDetailUrl, baseUrl.ToString(), proxyUrl);
            }
            else
            {
                var banDetailRequestUrl = $"{baseUrl.AbsoluteUri.TrimEnd('/')}{banDetailUrl}";
                status = await GetBanStatusAsyncWithoutProxy(context, telegenceAuth, banDetailRequestUrl);
            }

            return status;
        }

        private static async Task<string> GetTelegenceDeviceBySubscriberNumberByProxy(KeySysLambdaContext context, TelegenceAuthentication telegenceAuthentication, string deviceDetailUrl, string proxyUrl, string baseUrl)
        {
            AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.REQUEST_GET_DEVICE_DETAIL, deviceDetailUrl));
            var headerContent = BuildHeaderContent(telegenceAuthentication);
            var headerContentString = JsonConvert.SerializeObject(headerContent);
            var payload = BuildPayloadModel(deviceDetailUrl, baseUrl, headerContentString);
            var responseMessage = await Amop.Core.Helpers.RetryPolicyHelper.PollyRetryForProxyRequestAsync(context.logger, CommonConstants.NUMBER_OF_TELEGENCE_RETRIES).ExecuteAsync(async () =>
            {
                using (var client = new HttpClient())
                {
                    ConfigHttpClient(client);
                    var responseContent = MappingProxyResponseContent(client.GetWithProxy(proxyUrl, payload, context.logger));
                    return await Task.FromResult(responseContent);
                }
            });
            var responseBody = responseMessage.ResponseMessage;
            if (responseMessage.IsSuccessful)
            {
                AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.REQUEST_URL_SUCCESS, deviceDetailUrl));
                return responseBody;
            }
            AwsFunctionBase.LogInfo(context, LogTypeConstant.Error, string.Format(LogCommonStrings.REQUEST_FAILED_RESPONSE_AT_FINAL_RETRIES, deviceDetailUrl, responseBody));
            return string.Empty;
        }

        private static async Task<string> GetTelegenceDeviceBySubscriberNumberWithoutProxy(KeySysLambdaContext context, TelegenceAuthentication telegenceAuthentication, string deviceDetailUrl)
        {
            AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.REQUEST_GET_DEVICE_DETAIL, deviceDetailUrl));
            var responseMessage = await Amop.Core.Helpers.RetryPolicyHelper.PollyRetryHttpRequestAsync(context.logger, CommonConstants.NUMBER_OF_TELEGENCE_RETRIES).ExecuteAsync(async () =>
            {
                using (var client = new HttpClient())
                {
                    ConfigHttpClient(client);
                    BuildRequestHeaders(client, telegenceAuthentication);
                    return await client.GetAsync(deviceDetailUrl);
                }
            });
            var responseBody = responseMessage.Content.ReadAsStringAsync().Result;
            if (responseMessage.IsSuccessStatusCode)
            {
                AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.REQUEST_URL_SUCCESS, deviceDetailUrl));
                return responseBody;
            }
            AwsFunctionBase.LogInfo(context, LogTypeConstant.Error, string.Format(LogCommonStrings.REQUEST_FAILED_RESPONSE_AT_FINAL_RETRIES, deviceDetailUrl, responseBody));
            return string.Empty;
        }
        private static async Task<TelegenceGetDevicesSyncState> GetTelegenceDevicesAsyncByProxy(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, TelegenceAuthentication telegenceAuth, string proxyUrl,
           List<TelegenceDeviceResponse> telegenceDeviceList, int pageSize, string deviceDetailUrl, string baseUrl)
        {
            AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.REQUEST_GET_DEVICE, deviceDetailUrl));
            var headerContent = BuildHeaderContent(telegenceAuth);
            headerContent.Add(CommonConstants.CURRENT_PAGE, syncState.CurrentPage.ToString());
            headerContent.Add(CommonConstants.PAGE_SIZE, pageSize);
            var headerContentString = JsonConvert.SerializeObject(headerContent);

            var payload = BuildPayloadModel(deviceDetailUrl, baseUrl, headerContentString);
            var responseMessage = await Amop.Core.Helpers.RetryPolicyHelper.PollyRetryForProxyRequestAsync(context.logger, CommonConstants.NUMBER_OF_TELEGENCE_RETRIES).ExecuteAsync(async () =>
            {
                using (var client = new HttpClient())
                {
                    ConfigHttpClient(client);
                    var responseContent = MappingProxyResponseContent(client.GetWithProxy(proxyUrl, payload, context.logger));
                    return await Task.FromResult(responseContent);
                }
            });
            var responseBody = responseMessage.ResponseMessage;
            if (responseMessage.IsSuccessful)
            {
                List<TelegenceDeviceResponse> deviceList = JsonConvert.DeserializeObject<List<TelegenceDeviceResponse>>(responseBody);
                var headers = JsonConvert.DeserializeObject<ExpandoObject>(responseMessage.HeaderContent) as IDictionary<string, object>;

                if (int.TryParse(headers[CommonConstants.PAGE_TOTAL].ToString(), out int pageTotal))
                {
                    syncState.HasMoreData = syncState.CurrentPage < pageTotal;
                }

                syncState.IsLastCycle = !syncState.HasMoreData;

                if (DateTime.TryParse(headers[CommonConstants.REFRESH_TIMESTAMP].ToString(), out DateTime refreshTimestamp))
                {
                    GetTelegenceDeviceList(deviceList, telegenceDeviceList, syncState, refreshTimestamp);
                }
                AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.REQUEST_URL_SUCCESS, deviceDetailUrl));
            }
            else
            {
                syncState.IsLastCycle = true;
                AwsFunctionBase.LogInfo(context, LogTypeConstant.Error, string.Format(LogCommonStrings.REQUEST_FAILED_RESPONSE_AT_FINAL_RETRIES, deviceDetailUrl, responseBody));
            }
            return syncState;
        }

        private static async Task<TelegenceGetDevicesSyncState> GetTelegenceDevicesAsyncWithoutProxy(KeySysLambdaContext context, TelegenceAuthentication telegenceAuth, TelegenceGetDevicesSyncState syncState,
           List<TelegenceDeviceResponse> telegenceDeviceList, string telegenceDevicesGetUrl, int pageSize)
        {
            AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.REQUEST_GET_DEVICE, telegenceDevicesGetUrl));
            var responseMessage = await Amop.Core.Helpers.RetryPolicyHelper.PollyRetryHttpRequestAsync(context.logger, CommonConstants.NUMBER_OF_TELEGENCE_RETRIES).ExecuteAsync(async () =>
            {
                using (var client = new HttpClient())
                {
                    ConfigHttpClient(client);
                    BuildRequestHeaders(client, telegenceAuth);
                    client.DefaultRequestHeaders.Add(CommonConstants.CURRENT_PAGE, syncState.CurrentPage.ToString());
                    client.DefaultRequestHeaders.Add(CommonConstants.PAGE_SIZE, pageSize.ToString());
                    return await client.GetAsync(telegenceDevicesGetUrl);
                }
            });
            var responseBody = await responseMessage.Content.ReadAsStringAsync();
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
                AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.REQUEST_URL_SUCCESS, telegenceDevicesGetUrl));
            }
            else
            {
                syncState.IsLastCycle = true;
                AwsFunctionBase.LogInfo(context, LogTypeConstant.Error, string.Format(LogCommonStrings.REQUEST_FAILED_RESPONSE_AT_FINAL_RETRIES, telegenceDevicesGetUrl, responseBody));
            }
            return syncState;
        }

        private static async Task<string> GetBanStatusAsyncByProxy(KeySysLambdaContext context, TelegenceAuthentication telegenceAuth, string banDetailUrl, string baseUrl, string proxyUrl)
        {
            AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.REQUEST_GET_BAN_STATUS, banDetailUrl));
            var headerContent = BuildHeaderContent(telegenceAuth);
            var headerContentString = JsonConvert.SerializeObject(headerContent);
            var payload = BuildPayloadModel(banDetailUrl, baseUrl, headerContentString);
            var responseMessage = await Amop.Core.Helpers.RetryPolicyHelper.PollyRetryForProxyRequestAsync(context.logger, CommonConstants.NUMBER_OF_TELEGENCE_RETRIES).ExecuteAsync(async () =>
            {
                using (var client = new HttpClient())
                {
                    ConfigHttpClient(client);
                    var responseContent = MappingProxyResponseContent(client.GetWithProxy(proxyUrl, payload, context.logger));
                    return await Task.FromResult(responseContent);
                }
            });
            var responseBody = responseMessage.ResponseMessage;
            if (responseMessage.IsSuccessful)
            {
                AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.REQUEST_URL_SUCCESS, banDetailUrl));
                return GetBillingAccountStatus(responseBody);
            }
            AwsFunctionBase.LogInfo(context, LogTypeConstant.Error, string.Format(LogCommonStrings.REQUEST_FAILED_RESPONSE_AT_FINAL_RETRIES, banDetailUrl, responseBody));
            return string.Empty;
        }

        private static async Task<string> GetBanStatusAsyncWithoutProxy(KeySysLambdaContext context, TelegenceAuthentication telegenceAuth, string banDetailUrl)
        {
            AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.REQUEST_GET_BAN_STATUS, banDetailUrl));
            var responseMessage = await Amop.Core.Helpers.RetryPolicyHelper.PollyRetryHttpRequestAsync(context.logger, CommonConstants.NUMBER_OF_TELEGENCE_RETRIES).ExecuteAsync(async () =>
            {
                using (var client = new HttpClient())
                {
                    ConfigHttpClient(client);
                    BuildRequestHeaders(client, telegenceAuth);
                    return await client.GetAsync(banDetailUrl);
                }
            });
            var responseBody = await responseMessage.Content.ReadAsStringAsync();
            if (responseMessage.IsSuccessStatusCode)
            {
                AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.REQUEST_URL_SUCCESS, banDetailUrl));
                return GetBillingAccountStatus(responseBody);
            }
            AwsFunctionBase.LogInfo(context, LogTypeConstant.Error, string.Format(LogCommonStrings.REQUEST_FAILED_RESPONSE_AT_FINAL_RETRIES, banDetailUrl, responseBody));
            return string.Empty;
        }
        private static void BuildRequestHeaders(HttpClient client, TelegenceAuthentication telegenceAuth)
        {
            client.DefaultRequestHeaders.Add(CommonConstants.ACCEPT, CommonConstants.APPLICATION_JSON);
            client.DefaultRequestHeaders.Add(CommonConstants.APP_ID, telegenceAuth.ClientId);
            client.DefaultRequestHeaders.Add(CommonConstants.APP_SECRET, telegenceAuth.ClientSecret);
        }

        private static IDictionary<string, object> BuildHeaderContent(TelegenceAuthentication telegenceAuth)
        {
            var headerContent = new ExpandoObject() as IDictionary<string, object>;
            headerContent.Add(CommonConstants.APP_ID, telegenceAuth.ClientId);
            headerContent.Add(CommonConstants.APP_SECRET, telegenceAuth.ClientSecret);
            return headerContent;
        }

        private static PayloadModel BuildPayloadModel(string endPoint, string baseUrl, string headerContentString)
        {
            return new PayloadModel()
            {
                AuthenticationType = AuthenticationType.TELEGENCEAUTH,
                Endpoint = endPoint,
                HeaderContent = headerContentString,
                JsonContent = null,
                Password = null,
                Token = null,
                Url = baseUrl,
                Username = null
            };
        }

        private static void ConfigHttpClient(HttpClient httpClient)
        {
            httpClient.Timeout = TimeSpan.FromMinutes(CommonConstants.HTTP_CLIENT_REQUEST_TIMEOUT_IN_MINUTES);
        }

        private static Amop.Core.Models.ProxyResultBase MappingProxyResponseContent(ProxyResultBase proxyResponseContent)
        {
            return new Amop.Core.Models.ProxyResultBase
            {
                HeaderContent = proxyResponseContent.HeaderContent,
                IsSuccessful = proxyResponseContent.IsSuccessful,
                ResponseMessage = proxyResponseContent.ResponseMessage,
                StatusCode = proxyResponseContent.StatusCode
            };
        }
        private static string GetBillingAccountStatus(string responseBody)
        {
            var billingAccountDetail = JsonConvert.DeserializeObject<TelegenceBillingAccountDetailResponse>(responseBody);
            return billingAccountDetail?.Status;
        }

        private static void GetTelegenceDeviceList(List<TelegenceDeviceResponse> deviceList, List<TelegenceDeviceResponse> telegenceDeviceList, TelegenceGetDevicesSyncState syncState, DateTime refreshTimestamp)
        {
            var deviceTotal = 0;
            if (deviceList != null)
            {
                deviceTotal = deviceList.Count;
            }
            for (int i = 0; i < deviceTotal; i++)
            {
                deviceList[i].RefreshTimestamp = refreshTimestamp;
                telegenceDeviceList.Add(deviceList[i]);
            }
        }
    }
}
