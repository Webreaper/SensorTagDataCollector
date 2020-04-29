using System;
using System.Collections.Generic;
using RestSharp;
using SensorTagElastic;

namespace WirelessSensorTag
{
    public class GetStatsRawByUUIDBody
    {
        public const string GetStatsRestMethod = "ethLogShared.asmx/GetStatsRawByUUID";
        public const string GetTemperatureRawDataByUUID = "ethLogShared.asmx/GetTemperatureRawDataByUUID";

        public string uuid { get; set; }
        public DateTime fromDate { get; set; }
        public DateTime toDate { get; set; }
        public string name { get; set; }
    }


    public class StatRawDay
    {
        public DateTime date { get; set; }
        public List<DateTime> tods { get; set; }
        public List<double> temps { get; set; }
        public List<double> caps { get; set; }
    }

    public class SensorTagData
    {
        public List<StatRawDay> d { get; set; }
    }

    public class TemperatureDataPoint
    {
        public DateTime time { get; set; }
        public double temp_degC { get; set; }
        public double cap { get; set; }
        public double lux { get; set; }
        public double battery_volts { get; set; }
    }

    public class RawTempData
    {
        public List<TemperatureDataPoint> d { get; set; }
        public string name { get; set; }
    }

    public class TagInfo
    {
        public int slaveId { get; set; }
        public string name { get; set; }
        public string uuid { get; set; }
        public double batteryVolt { get; set; }
        public double batteryRemaining { get; set; }
    }

    public class TagList
    {
        public List<TagInfo> d { get; set; }
    }

    public class WirelessSensorTagAPI
    {
        private const string CookieWTAG = "WTAG";
        private ServerCertificateValidation certValidation = new ServerCertificateValidation();
        private readonly RestClient wstclient;
        private IList<RestResponseCookie> cookies = null;

        public WirelessSensorTagAPI(string url)
        {
            wstclient = new RestClient(url);
        }

        public bool SignIn(string userName, string pw)
        {
            bool success = true;

            var body = new { email = userName, password = pw };

            var data = MakeRestRequest<object>("ethAccount.asmx/SignIn", body);

            return success;
        }

        public RawTempData GetTemperatureRawDataByUUID(GetStatsRawByUUIDBody requestObject)
        {
            Utils.Log("Querying data between {0} and {1}...", requestObject.fromDate, requestObject.toDate, requestObject.uuid);

            var data = MakeRestRequest<RawTempData>(GetStatsRawByUUIDBody.GetTemperatureRawDataByUUID, requestObject);

            return data;
        }

        public T MakeRestRequest<T>(string requestMethod, object requestBody) where T : new()
        {
            var request = new RestRequest(requestMethod, Method.POST);
            if (requestBody != null)
                request.AddJsonBody(requestBody);

            if (cookies != null)
                request.AddCookie(cookies[0].Name, cookies[0].Value);

            T data = default(T);

            try
            {
                var queryResult = wstclient.Execute<T>(request);

                if (queryResult != null)
                {
                    if (queryResult.StatusCode != System.Net.HttpStatusCode.OK)
                    {
                        Utils.Log("Error: {0} - {1}", queryResult.StatusCode, queryResult.Content);
                    }
                    else
                    {
                        data = queryResult.Data;

                        if (cookies == null)
                        {
                            Utils.Log("Storing cookies from Auth request: {0}", requestMethod);
                            cookies = (IList<RestResponseCookie>)queryResult.Cookies;
                        }
                    }
                }
                else
                    Utils.Log("No valid queryResult.");
            }
            catch (Exception ex)
            {
                Utils.Log("Exception: {0}: {1}", ex.Message, ex);
            }

            return data;
        }
    }
}
