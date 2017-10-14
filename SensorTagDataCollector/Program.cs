﻿using System;
using System.Linq;
using System.Collections.Generic;
using WirelessSensorTag;
using Nest;
using System.IO;
using System.Threading;
using Hive;
using Weather;

namespace SensorTagElastic
{
    /// <summary>
    /// TODO:
    /// 1. 24-hour expiry of sessions
    /// 2. Limit Hive queries to min 2-minute refresh
    /// 3. Separate indexes for hive and sensor datas
    /// 4. Tidy up code and refactor
    /// 5. Alerts when data not received for certain period
    /// 6. Setting in json file for battery warning threshold
    /// 7. Record battery from Hive
    /// 8. 
    /// </summary>
    public class MainClass
    {
        [ElasticsearchType]
        public class Reading 
        {
            public string Id { get; set; }
            public DateTime timestamp { get; set; }
            public DateTime ingestionTimeStamp { get; set; }
        }

        [ElasticsearchType]
        public class WeatherReading : Reading
        {
            public double temperature { get; set; }
            public double humidity { get; set; }
            public double rainfall { get; set; }
            public double snowfall { get; set; }
            public double windspeed { get; set; }
            public string windDirection { get; set; }
            public bool thunder { get; set; }
            public bool fog { get; set; }
            public bool hail { get; set; }
        }

        [ElasticsearchType]
        public class WeatherSummary : Reading
        {
            public double rainfallMM { get; set; }
            public double snowfallMM { get; set; }
            public double maxTempC { get; set; }
            public double minTempC { get; set; }
            public double maxRH { get; set; }
            public double minRH { get; set; }
            public double maxWindSpeedKMH { get; set; }
            public double minWindSpeedKMH { get; set; }
        }

        [ElasticsearchType]
        public class SensorReading : Reading
        {
            public SensorDevice device { get; set; }
            public double? temperature { get; set; }
            public double? humidity { get; set; }
            public double? lux { get; set; }
            public double? battery { get; set; }
            public int? batteryPercentage { get; set; }
            // Lux adjusted with a floor of 1 to allow log scale
            public double luxAdjusted { get { if (lux.HasValue) return Math.Max(lux.Value, 1); else return 1; } }

            public override string ToString()
            {
                return string.Format("[SensorReading: timestamp={0}, device={1}, temperature={2}]", timestamp, device.name, temperature, humidity, lux, battery);
            }
        }

        public class Alert
        {
            public SensorDevice device { get; set; }
            public string alertText { get; set; }
        }

        private ElasticClient EsClient;
        private Settings settings;

        /// <summary>
        /// Find the timestamp of the most recent record for each device. 
        /// </summary>
        /// <returns>The high water mark.</returns>
        /// <param name="uuid">Device UUID</param>
        private DateTime getDeviceHighWaterMark( string uuid )
        {
            var query = new TermQuery { Field = "device.uuid.keyword", Value = uuid };

            Utils.Log("Getting high water mark for {0}...", uuid);
        
            var mostRecent = ElasticUtils.getHighWaterMark<SensorReading>(EsClient, settings.indexname, query);

            if (mostRecent != null)
            {
                if (mostRecent.device.uuid != uuid)
                    throw new ArgumentException("High watermark returned for incorrect UUID!");

                return mostRecent.timestamp.AddSeconds(1);
            }

            return new DateTime(2017, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        }

        /// <summary>
        /// Find the timestamp of the most recent record for each device. 
        /// </summary>
        /// <returns>The high water mark.</returns>
        private DateTime getWeatherHighWaterMark()
        {
            Utils.Log("Getting high water mark for weather...");

            var mostRecent = ElasticUtils.getHighWaterMark<WeatherReading>(EsClient, settings.weatherUnderground.IndexName, null);

            if (mostRecent != null)
            {
                return mostRecent.timestamp.AddSeconds(1);
            }

            return new DateTime(2014, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        }

        /// <summary>
        /// Queries the data in the sensor tag.
        /// </summary>
        public ICollection<SensorReading> QuerySensorTags(WirelessSensorTagAPI tagService, List<SensorDevice> allDevices)
        {
            var result = new List<SensorReading>();

            Utils.Log("Querying tag list...");
            var tagList = tagService.MakeRestRequest<TagList>("ethClient.asmx/GetTagList", new { });

            if (tagList.d.Any())
            {
                foreach (var tag in tagList.d)
                {
                    var fromDate = getDeviceHighWaterMark(tag.uuid);
                    var toDate = DateTime.UtcNow;

                    Utils.Log("Querying {0} data between {1} and {2}...", tag.name, fromDate, toDate);

                    var body = new { id = tag.slaveId, fromDate, toDate };
                    var data = tagService.MakeRestRequest<RawTempData>("ethLogs.asmx/GetTemperatureRawData", body);

                    if (data != null && data.d != null)
                    {
                        SensorDevice device = new SensorDevice { uuid = tag.uuid, name = tag.name, type = "Tag", location = "" };
                        allDevices.Add(device);
                        var records = CreateSensorData(device, data);

                        var firstReading = records.Min(x => x.timestamp);
                        var lastReading = records.Max(x => x.timestamp);
                        Utils.Log("Found readings from {0:dd-MMM-yyyy HH:mm:ss} to {1:dd-MMM-yyyy HH:mm:ss}", firstReading, lastReading);

                        var newRecords = records.Where(x => x.timestamp > fromDate).ToList();

                        if (newRecords.Any())
                        {
                            Utils.Log("Filtered {0} previously-seen records - storing {1}", records.Count() - newRecords.Count(), newRecords.Count());

                            result.AddRange(newRecords);
                        }
                        else
                            Utils.Log("All records were older than high watermark. Ignoring.");
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Queries the data in the sensor tag.
        /// </summary>
        public ICollection<SensorReading> QueryHiveData(HiveService service, List<SensorDevice> allDevices)
        {

            List<SensorReading> readings = new List<SensorReading>();

            var hiveBattChannel = service.GetHiveChannel(HiveService.ChannelType.battery);
            var hiveTempChannel = service.GetHiveChannel(HiveService.ChannelType.temperature);

            var fromDate = getDeviceHighWaterMark(hiveTempChannel.UUID);
            var toDate = DateTime.UtcNow;

            SensorDevice device = new SensorDevice { uuid = hiveTempChannel.UUID, name = "Hive", type = "HiveHome", location = "" };
            allDevices.Add(device);

            Utils.Log("Querying hive {0} data between {1} and {2}...", hiveTempChannel.id, fromDate, toDate);

            var values = service.QueryHiveValues(hiveTempChannel, device, fromDate, toDate, settings.refreshPeriodMins);
            var battValues = service.QueryHiveValues(hiveBattChannel, device, fromDate, toDate, settings.refreshPeriodMins);

            if (values.Any())
            {
                SensorReading lastReading = null;

                foreach (var pair in values)
                {
                    lastReading = new SensorReading
                    {
                        timestamp = Utils.getFromEpoch(pair.Key),
                        temperature = pair.Value,
                        lux = null,
                        humidity = null,
                        device = device
                    };

                    if (battValues.ContainsKey(pair.Key))
                        lastReading.battery = battValues[pair.Key];
                    
                    readings.Add(lastReading);
                }

                if (readings.Any())
                {
                    Utils.Log("Found {0} readings for device '{1}' (latest: {2:dd-MMM-yy HH:mm:ss}).", readings.Count(), device.name, readings.Max(x => x.timestamp));
                }
                else
                    Utils.Log("No readings found for device {1}.", device.name);

                // Now query the current battery level, and set it in the most recent reading.
                lastReading.batteryPercentage = service.QueryHiveBattery();
            }

            return readings;
        }

        /// <summary>
        /// Given a device and a set of data returned from the SensorTag API, 
        /// transforms the data from arrays of values, into individual readings
        /// for each time, device and set of values. Then indexes them into 
        /// ElasticSearch.
        /// </summary>
        /// <param name="device">Device.</param>
        /// <param name="data">Data.</param>
        private ICollection<SensorReading> CreateSensorData(SensorDevice device, RawTempData data)
        {
            Utils.Log("Processing {0} data points...", data.d.Count());
            const double maxBatteryPercent = 3.0 / 100.0;

            List<SensorReading> readings = new List<SensorReading>();

            foreach (var datapoint in data.d)
            {
                readings.Add(new SensorReading
                {
                    timestamp = datapoint.time,
                    device = device,
                    temperature = datapoint.temp_degC,
                    lux = datapoint.lux,
                    humidity = datapoint.cap,
                    battery = datapoint.battery_volts,
                    batteryPercentage = (int)(datapoint.battery_volts / maxBatteryPercent)
                });
            }

            if (readings.Any())
                Utils.Log("Found {0} readings for device '{1}' (latest: {2:dd-MMM-yy HH:mm:ss}).", readings.Count(), device.name, readings.Max(x => x.timestamp));

            return readings;
        }

        /// <summary>
        /// Stores the sensor readings.
        /// </summary>
        /// <param name="readings">Readings.</param>
        private void StoreReadings(ICollection<Reading> readings, string indexName )
        {
            if (readings.Any())
            {
                var uniqueReadings = readings.Distinct().ToList();

                if (uniqueReadings.Count() < readings.Count())
                    Utils.Log("Conflated duplicate readings from {0} to {1}", readings.Count(), uniqueReadings.Count());
                
                Utils.Log("Indexing {0} sensor readings from {1} to {2}", uniqueReadings.Count(), uniqueReadings.First().timestamp, uniqueReadings.Last().timestamp);

                var years = uniqueReadings.GroupBy(x => x.timestamp.Year, (year, values) =>
                                            new { year, values = values.OrderBy(x => x.timestamp).ToList() })
                                    .ToList();

                var now = DateTime.UtcNow;

                foreach (var kvp in years)
                {
                    if (kvp.values.Any())
                    {
                        string yearIndex = string.Format("{0}-{1}", indexName, kvp.year);

                        foreach (var x in kvp.values)
                            x.ingestionTimeStamp = now;
                        
                        ElasticUtils.BulkInsert(EsClient, yearIndex, kvp.values);
                    }
                }

                ElasticUtils.createDateBasedAliases(EsClient, indexName);
            }
            else
                Utils.Log("No readings to ingest.");
        }

        /// <summary>
        /// Analyses the battery levels for all devices and pulls out the lowest battery 
        /// reading in the batch of sensor readings. If any devices have a battery level
        /// of less than 1.0, an email is sent warning for all devices.
        /// </summary>
        /// <param name="readings">Readings.</param>
        private void CheckBatteryStatus(ICollection<Reading> readings, int threshold, List<Alert> alerts)
        {
            Utils.Log("Checking battery status for devices...");

            var lowBatteryDevices = readings.Cast<SensorReading>()
                                            .Where(x => x.battery < threshold && x.battery > 0.0)
                                            .GroupBy(x => x.device,
                                                     y => y.battery,
                                                     (key, b) => new { device = key, lowestBattery = b.Min() })
                                            .ToList();

            if (lowBatteryDevices.Any())
            {
                string msg = "Warning. One or more devices are indicating a low battery status.\n\n";

                foreach (var status in lowBatteryDevices)
                {
                    var lastReading = readings.Max(x => x.timestamp);
                    msg += string.Format($"Low Battery - {status.lowestBattery}v (last reading: {1:dd-MMM-yyyy HH:mm:ss}\n", status.lowestBattery, lastReading);
                    alerts.Add(new Alert { device = status.device, alertText = msg });
                }
            }
        }

        /// <summary>
        /// Main work method
        /// </summary>
        /// <param name="settings">Settings.</param>
        public void ProcessTags(Settings settings)
        {
            this.settings = settings;
            Uri esPath = new UriBuilder
            {
                Host = settings.elasticserver,
                Port = 9200
            }.Uri;

            EsClient = ElasticUtils.getElasticClient(esPath, settings.indexname, false);

            try
            {
                var weatherReadings = QueryWeatherData( settings.weatherUnderground );

                StoreReadings( weatherReadings, settings.weatherUnderground.IndexName );
            }
            catch( Exception ex )
            {
                Utils.Log("Exception querying Weather data. {0}", ex);
            }

            List<Reading> allReadings = new List<Reading>();
            List<SensorDevice> allDevices = new List<SensorDevice>();

            try
            {
                if (settings.hive != null)
                {
                    HiveService service = new HiveService();

                    if (service.SignIn(settings.hive.username, settings.hive.password))
                    {

                        var hiveReadings = QueryHiveData(service, allDevices);

                        allReadings.AddRange(hiveReadings);
                    }
                }
            }
            catch( Exception ex )
            {
                Utils.Log( "Exception querying Hive data. {0}", ex);
            }

            try
            {
                if (settings.wirelesstag != null)
                {
                    WirelessSensorTagAPI tagService = new WirelessSensorTagAPI(settings.wirelesstag.WirelessTagServiceUrl);

                    if (tagService.SignIn(settings.wirelesstag.username, settings.wirelesstag.password))
                    {
                        var wirelessTagReadings = QuerySensorTags(tagService, allDevices);

                        allReadings.AddRange(wirelessTagReadings);
                    }
                }
            }
            catch (Exception ex)
            {
                Utils.Log("Exception querying SensorTag data. {0}", ex);
            }

            if( allReadings.Any() && settings.email != null )
            {
                try
                {
                    StoreReadings(allReadings, settings.indexname);

                    ElasticUtils.DeleteDuplicates(EsClient, settings.indexname);
                }
                catch (Exception ex)
                {
                    Utils.Log("Exception ingesting data in ES. {0}", ex);
                }

                try
                {
                    List<Alert> alerts = new List<Alert>();

                    // See if any of the data we got back indicated a drained battery.
                    CheckBatteryStatus(allReadings, settings.lowBatteryThreshold, alerts);
                    CheckMissingData(allDevices, alerts);

                    if (alerts.Any())
                    {
                        Utils.SendAlertEmail(settings.email, alerts);
                    }
                }
                catch( Exception ex )
                {
                    Utils.Log("Exception checking missing data. {0}", ex);
                }
            }

            Utils.Log("Run complete.");
        }

        private ICollection<Reading> QueryWeatherData(WUGSettings weatherSettings)
        {
            WeatherService weather = new WeatherService(weatherSettings);
            var allReadings = new List<Reading>();

            var mostRecent = getWeatherHighWaterMark();
            var today = DateTime.Now;
            int totalCalls = 0;

            while (mostRecent < today && totalCalls < 200 )
            {
                Utils.Log("Querying weather for {0}", mostRecent);

                var readings = weather.GetHistory(mostRecent);
                allReadings.AddRange(readings);
                totalCalls++;

                // Max of 10 weather calls per minute
                Thread.Sleep(6 * 1000);
            }

            return allReadings;
        }

        /// <summary>
        /// Query ES for unique set of devices, then check when each last sent data back
        /// Get the list of Device UUIDs from the server so we get currently registered
        /// devices, not historical ones which clearly won't be transmitting. ;)
        /// </summary>
        private void CheckMissingData(IList<SensorDevice> allDevices, List<Alert> alerts )
        {
            Utils.Log("Checking for missing data from devices...");

            foreach( var device in allDevices )
            {
                var minsSinceLastReading = (DateTime.UtcNow - getDeviceHighWaterMark(device.uuid)).Minutes;

                Utils.Log("Device {0} was updated {1} minutes ago. ", device.name, minsSinceLastReading);
                if (minsSinceLastReading > settings.noDataWarningMins)
                {
                    alerts.Add(new Alert { device = device, alertText = $"No data for {minsSinceLastReading} minutes." });
                }
            }
        }

        public static void Main(string[] args)
        {
            var x = new MainClass();

            var settingPath = args.Where(p => p.EndsWith(".json", StringComparison.OrdinalIgnoreCase)).FirstOrDefault();

            if (string.IsNullOrEmpty(settingPath))
                settingPath = "Settings.json";
            
            try
            {
                string json = File.ReadAllText(settingPath);

                var settings = Utils.deserializeJSON<Settings>(json);

                Utils.logLocation = settings.logLocation;
                bool loop = settings.refreshPeriodMins > 0;

                do{
                    x.ProcessTags(settings);

                    if( loop )
                    {
                        Utils.Log("Sleeping for {0} minutes...", settings.refreshPeriodMins);
                        Thread.Sleep(1000 * 60 * settings.refreshPeriodMins); // Sleep for 1 minute
                    }
                }
                while (loop);
            
            }
            catch( Exception ex )
            {
                Utils.Log("Unable to initialise settings: {0}", ex.Message);                
            }
        }
    }
}
