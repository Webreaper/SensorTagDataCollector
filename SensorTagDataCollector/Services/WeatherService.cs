using System;
using System.Collections.Generic;
using System.Globalization;
using RestSharp;
using SensorTagElastic;
using static SensorTagElastic.MainClass;

namespace Weather
{
    public class WeatherService
    {
        private ServerCertificateValidation certValidation = new ServerCertificateValidation();
        private readonly RestClient wugClient;
        private readonly WUGSettings settings;

        public class WUGDate
        {
            public string pretty { get; set; }
            public string tzname { get; set; }
            public string year { get; set; }
            public string mon { get; set; }
            public string mday { get; set; }
            public string hour { get; set; }
            public string min { get; set; }
        }

        public class WUGDailySummary
        {
            public WUGDate date { get; set; }
    
            public int fog { get; set; }
            public int rain { get; set; }
            public int snow { get; set; }

            public string snowfallm { get; set; }
            public string snowdepthm { get; set; }
            public string meanwindspdm { get; set; }
            public string maxtempm { get; set; }
            public string mintempm { get; set; }
            public string maxhumidity { get; set; }
            public string minhumidity { get; set; }
            public string maxwspdm { get; set; }
            public string minwspdm { get; set; }
            public string precipm { get; set; }
            public string precipsource { get; set; }
        }

        public class WUGObservation
        {
            public WUGDate date { get; set; }

            public string fog { get; set; }
            public string rain { get; set; }
            public string snow { get; set; }
            public string hail { get; set; }
            public string thunder { get; set; }

            public string precipm { get; set; }
            public string tempm { get; set; }
            public string wspdm { get; set; }
            public string hum { get; set; }

            public string wdire { get; set; }
        }

        public class WUGHistory 
        {
            public WUGDate date { get; set; }
            public WUGDate utcdate { get; set; }
            public List<WUGObservation> observations { get; set; }
            public List<WUGDailySummary> dailysummary { get; set; }
         }

        public class WUGResponse 
        {
            public string version { get; set; }
        }

        public class WUGPayload
        {
            public WUGResponse response { get; set; }
            public WUGHistory history { get; set; }
        }

        public WeatherService( WUGSettings settings )
        {
            this.settings = settings;

            Uri uri = new Uri( "http://api.wunderground.com/api/" + settings.apiKey );
            wugClient = new RestClient(uri);

        }

        public bool GetHistory( DateTime date, IList<WeatherReading> weatherReadings, IList<WeatherSummary> summaryReadings )
        {
            string queryDate = string.Format("{0:yyyyMMdd}", date);
            string url = $"history_{queryDate}/q/{settings.Country}/{settings.City}.json";
            bool success = true;

            var data = MakeRestRequest<WUGPayload>(url);

            if (data != null && data.history != null)
            {
                if (data.history.observations.Count > 0)
                {
                    foreach (var obs in data.history.observations)
                    {
                        string fullDate = $"{obs.date.year}/{obs.date.mon}/{obs.date.mday} {obs.date.hour}:{obs.date.min}:00";
                        var parsedDate = DateTime.ParseExact(fullDate, "yyyy/MM/dd HH:mm:ss", CultureInfo.InvariantCulture);

                        WeatherReading reading = new WeatherReading
                        {

                            timestamp = parsedDate,
                            rainfall = obs.rain == "1" ? safeParseDouble(obs.precipm) : 0,
                            snowfall = obs.snow == "1" ? safeParseDouble(obs.precipm) : 0,
                            temperature = safeParseDouble(obs.tempm),
                            humidity = safeParseDouble(obs.hum),
                            windspeed = safeParseDouble(obs.wspdm),
                            windDirection = obs.wdire,
                            hail = (obs.hail == "1"),
                            thunder = (obs.thunder == "1"),
                            fog = (obs.fog == "1"),
                        };

                        weatherReadings.Add(reading);
                    }
                }

                if (data.history.dailysummary.Count > 0)
                {
                    foreach (var summary in data.history.dailysummary)
                    {
                        string fullDate = $"{summary.date.year}/{summary.date.mon}/{summary.date.mday} 00:00:00";
                        var parsedDate = DateTime.ParseExact(fullDate, "yyyy/MM/dd HH:mm:ss", CultureInfo.InvariantCulture);

                        WeatherSummary summaryReading = new WeatherSummary()
                        {

                            timestamp = parsedDate,
                            rainfallMM = safeParseDouble(summary.precipm),
                            snowfallMM = safeParseDouble(summary.snowfallm),
                            maxTempC = safeParseDouble(summary.maxtempm),
                            minTempC = safeParseDouble(summary.mintempm),
                            maxRH = safeParseDouble(summary.maxhumidity),
                            minRH = safeParseDouble(summary.minhumidity),
                            maxWindSpeedKMH = safeParseDouble(summary.maxwspdm),
                            minWindSpeedKMH = safeParseDouble(summary.minwspdm)
                        };

                        summaryReadings.Add(summaryReading);
                    }
                }
            }
            else
            {
                Utils.Log("No data returned from WUG API - possible API limit breach.");
                success = false;
            }

            return success;
        }

        private double safeParseDouble( string input )
        {
            double result = 0.0;
            if (double.TryParse(input, out result ))
                return result;

            return result;
        }

        public T MakeRestRequest<T>(string requestMethod ) where T : new()
        {
            var request = new RestRequest(requestMethod, Method.GET);

            T data = default(T);

            try
            {
                var queryResult = wugClient.Execute<T>(request);

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
