using System;
using System.Collections.Generic;
using RestSharp;
using SensorTagElastic;
using static SensorTagElastic.MainClass;

namespace Hive
{
    public class HiveService
    {
        private const string CookieWTAG = "WTAG";
        private ServerCertificateValidation certValidation = new ServerCertificateValidation();
        private readonly RestClient hiveClient;
        private List<KeyValuePair<string, string>> headers;

        public class HiveAttribute
        {
            public double reportedValue { get; set; }
            public double displayValue { get; set; }
            public long reportReceivedTime { get; set; } 
            public long reportChangedTime { get; set; }
        }

        public class HiveTemperatureSensor
        {
            public HiveAttribute temperature { get; set; }
            public HiveAttribute batteryLevel { get; set; }
        }

        public class HiveFeatureCollection
        {
            public HiveTemperatureSensor temperature_sensor_v1 { get; set; }
            public HiveTemperatureSensor battery_device_v1 { get; set; }
        }

        public class HiveNode
        {
            public string name { get; set; }
            public string id { get; set; }
            public HiveFeatureCollection features { get; set; }
            public object attributes { get; set; }
        }

        public class HiveResponse
        {
            public List<HiveNode> nodes { get; set; }
        }

        public class HiveAuthSession
        {
            public string username { get; set; }
            public string sessionId { get; set; }
        }

        public class HiveAuthResponse
        {
            public List<HiveAuthSession> sessions { get; set; }
        }

        public class HiveAuth
        {
            public string username { get; set; }
            public string password { get; set; }
            public string caller { get; set; }
        }

        public class HiveAuthRequest
        {
            public List<HiveAuth> sessions { get; set; }
        }


        public class HiveChannelResponse
        {
            public List<HiveChannel> channels { get; set; }    
        }

        public class HiveChannel
        {
            public string id { get; set; } 
            public string unit { get; set; }
            public long start { get; set; }
            public long end { get; set; }
            public string timeUnit { get; set; }
            public int rate { get; set; }

            public List<string> supportedOperations { get; set; }
            public Dictionary<long,double> values { get; set; }

            public string UUID { get { return id.Split("@".ToCharArray())[1]; } }
        }

        public enum ChannelType
        {
            temperature,
            battery
        }

        public HiveService()
        {
            string url = "https://api-prod.bgchprod.info:443/omnia";
            hiveClient = new RestClient(url);

            headers = new List<KeyValuePair<string, string>>();

            headers.Add(new KeyValuePair<string, string>("Content-Type", "application/vnd.alertme.zoo-6.5+json"));
            headers.Add(new KeyValuePair<string, string>("Accept", "application/vnd.alertme.zoo-6.5+json"));
            headers.Add(new KeyValuePair<string, string>("X-Omnia-Client", "Hive Web Dashboard"));

        }

        public bool SignIn(string userName, string pw)
        {
            bool success = true;

            var body = new HiveAuthRequest{ sessions = new List<HiveAuth> { 
                    new HiveAuth{ username = userName, password = pw, caller = "WEB" }}
            };

            var data = MakeRestRequest<HiveAuthResponse>("auth/sessions", body);

 
            if (data.sessions.Count > 0)
            {
                Utils.Log("Storing sessionID from request");
                headers.Add(new KeyValuePair<string, string>("X-Omnia-Access-Token", data.sessions[0].sessionId));
            }

            return success;
        }

        public int QueryHiveBattery()
        {
            double batteryLevel = -1;
            var request = new RestRequest("nodes", Method.GET);

            foreach (var header in headers)
                request.AddHeader(header.Key, header.Value);

            var queryResult = hiveClient.Execute<HiveResponse>(request);

            if (queryResult != null)
            {
                if (queryResult.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    Utils.Log("Error: {0} - {1}", queryResult.StatusCode, queryResult.Content);
                }
                else
                {
                    try
                    {

                        foreach (var node in queryResult.Data.nodes)
                        {
                            if (node.name.Equals("Your Receiver"))
                            {
                                var x = node.features.temperature_sensor_v1;

                                if (x != null && x.temperature != null)
                                {
                                    double currentTemp = x.temperature.displayValue;
                                    Utils.Log("Hive temp at {0:dd-MMM-yyyy HH:mm:ss} => {1}", Utils.getFromEpoch(x.temperature.reportChangedTime), currentTemp);
                                }
                            }

                            if (node.name.Equals("Your Thermostat"))
                            {
                                var x = node.features.battery_device_v1;

                                if (x != null && x.batteryLevel != null)
                                {
                                    batteryLevel = x.batteryLevel.displayValue;
                                    Utils.Log("Hive battery at {0:dd-MMM-yyyy HH:mm:ss} => {1}", Utils.getFromEpoch(x.batteryLevel.reportChangedTime), batteryLevel);
                                }
                            }
                        }
                    }
                    catch( Exception ex )
                    {
                        Utils.Log("Exception extracting Hive temperature/battery: {0}", ex);
                    }
                }
            }
            else
                Utils.Log("No valid queryResult.");
            
            return (int)batteryLevel;
        }

        public HiveChannel GetHiveChannel(ChannelType channelType)
        {
            var request = new RestRequest("channels", Method.GET);

            foreach (var header in headers)
                request.AddHeader(header.Key, header.Value);

            var queryResult = hiveClient.Execute<HiveChannelResponse>(request);

            foreach( var channel in queryResult.Data.channels )
            {
                if( channel.id.StartsWith(channelType.ToString() + "@", StringComparison.OrdinalIgnoreCase))
                {
                    return channel;
                }
            }
            return null;
        }

        public Dictionary<long, double> QueryHiveValues(HiveChannel channel, SensorDevice device, DateTime fromDate, DateTime toDate, int refreshIntervalMins)
        {
            var request = new RestRequest("channels/" + channel.id, Method.GET);

            foreach (var header in headers)
                request.AddHeader(header.Key, header.Value);
            
            request.AddParameter("start", Utils.getEpochTime( fromDate ) );
            request.AddParameter("end", Utils.getEpochTime(toDate));
            request.AddParameter("rate", 5); 
            request.AddParameter("timeUnit", "MINUTES");
            request.AddParameter("operation", "AVG");

            var queryResult = hiveClient.Execute<HiveChannelResponse>(request);

            if (queryResult.StatusCode == System.Net.HttpStatusCode.OK)
            {
                if( queryResult.Data != null )
                {
                    return queryResult.Data.channels[0].values;
                }
                else
                    Utils.Log("Error! No channels returned from Hive call.");
            }
            else
            {
                Utils.Log("Error: {0} - {1}", queryResult.StatusCode, queryResult.Content);
            }

            return new Dictionary<long, double>();
        }

        public T MakeRestRequest<T>(string requestMethod, object requestBody ) where T : new()
        {
            var request = new RestRequest(requestMethod, Method.POST);

            foreach( var header in headers)
                request.AddHeader(header.Key, header.Value);

            if( requestBody != null )
                request.AddJsonBody(requestBody);

            T data = default(T);

            try
            {
                var queryResult = hiveClient.Execute<T>(request);

                if (queryResult != null)
                {
                    if (queryResult.StatusCode != System.Net.HttpStatusCode.OK)
                    {
                        Utils.Log("Error: {0} - {1}", queryResult.StatusCode, queryResult.Content);
                    }
                    else
                    {
                        data = queryResult.Data;
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
