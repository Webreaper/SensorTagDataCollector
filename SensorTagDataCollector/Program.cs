using System;
using System.Linq;
using System.Collections.Generic;
using WirelessSensorTag;
using Nest;
using System.IO;
using System.Threading;
using Hive;
using Weather;
using Logging;

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
            public string deviceName { get; set; }
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
        
            var mostRecent = EsClient.getHighWaterMark<SensorReading>(settings.indexname, query);

            if (mostRecent != null)
            {
                if (mostRecent.device.uuid != uuid)
                    throw new ArgumentException("High watermark returned for incorrect UUID!");

                return mostRecent.timestamp.AddSeconds(1);
            }

            return new DateTime(2017, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        }

        /// <summary>
        /// Find the timestamp of the most recent record for each device. 
        /// </summary>
        /// <returns>The high water mark.</returns>
        private DateTime getWeatherHighWaterMark()
        {
            Utils.Log("Getting high water mark for weather...");

            var mostRecent = EsClient.getHighWaterMark<WeatherReading>(settings.weatherUnderground.IndexName, null);

            if (mostRecent != null)
            {
                return mostRecent.timestamp.AddSeconds(1);
            }

            return new DateTime(2014, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        }

        /// <summary>
        /// Find the timestamp of the most recent record for each device. 
        /// </summary>
        /// <returns>The high water mark.</returns>
        private DateTime getSummaryHighWaterMark()
        {
            Utils.Log("Getting high water mark for weather...");

            var mostRecent = EsClient.getHighWaterMark<WeatherSummary>(settings.weatherUnderground.SummaryIndexName, null);

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
                    int dateRangeForBatch = 90;
                    var fromDate = getDeviceHighWaterMark(tag.uuid);
                    var toDate = DateTime.UtcNow;
                    bool gotRecords = false;

                    while (!gotRecords)
                    {
                        // Limit to 90 days at a time
                        if ((toDate - fromDate).TotalDays > dateRangeForBatch)
                            toDate = fromDate.AddDays(dateRangeForBatch);

                        Utils.Log("Querying {0} data between {1:dd-MMM-yyyy} and {2:dd-MMM-yyyy}...", tag.name, fromDate, toDate);

                        var body = new { id = tag.slaveId, fromDate, toDate };
                        var data = tagService.MakeRestRequest<RawTempData>("ethLogs.asmx/GetTemperatureRawData", body);

                        if (data != null && data.d != null)
                        {
                            SensorDevice device = new SensorDevice { uuid = tag.uuid, name = tag.name, type = "Tag", location = "" };
                            allDevices.Add(device);
                            var records = CreateSensorData(device, data, tag);

                            if (records.Any())
                            {
                                gotRecords = true;

                                var firstReading = records.Min(x => x.timestamp);
                                var lastReading = records.Max(x => x.timestamp);
                                Utils.Log("Found readings from {0:dd-MMM-yyyy HH:mm:ss} to {1:dd-MMM-yyyy HH:mm:ss}", firstReading, lastReading);

                                var newRecords = records.Where(x => x.timestamp > fromDate).ToList();

                                if (newRecords.Any())
                                {
                                    Utils.Log("Filtered {0} previously-seen records - {1} new", records.Count() - newRecords.Count(), newRecords.Count());

                                    result.AddRange(newRecords);
                                }
                                else
                                    Utils.Log("All records were older than high watermark. Ignoring.");
                            }
                        }

                        // Throttle to ensure we don't hit the sensortag server too hard.
                        Utils.Log("Sleeping for 10s to throttle requests.");
                        Thread.Sleep(10 * 1000);

                        if( ! gotRecords )
                        {
                            fromDate = toDate;
                            toDate = DateTime.UtcNow;

                            var diff = toDate - fromDate;
                            // Up to date
                            if (diff.TotalMinutes < 60 )
                                break;

                            Utils.Log("No data in date range. Trying next window.");
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Queries the data in the sensor tag.
        /// </summary>
        public ICollection<SensorReading> QueryHiveData(HiveService service )
        {

            List<SensorReading> readings = new List<SensorReading>();

            var hiveBattChannel = service.GetHiveChannel(HiveService.ChannelType.battery);
            var hiveTempChannel = service.GetHiveChannel(HiveService.ChannelType.temperature);

            var fromDate = getDeviceHighWaterMark(hiveTempChannel.UUID);
            var toDate = DateTime.UtcNow;

//            if ((toDate - fromDate).TotalDays > 90)
//                toDate = fromDate.AddDays(90);

            var device = new SensorDevice { uuid = hiveTempChannel.UUID, name = "Hive", type = "HiveHome", location = "" };

            Utils.Log("Querying hive {0} data between {1} and {2}...", hiveTempChannel.id, fromDate, toDate);

            var values = service.QueryHiveValues(hiveTempChannel, device, fromDate, toDate, settings.refreshPeriodMins);

            var battValues = new Dictionary<long, double>();
            if( hiveBattChannel != null )
                battValues = service.QueryHiveValues(hiveBattChannel, device, fromDate, toDate, settings.refreshPeriodMins);

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
                    Utils.Log("No readings found for device {0}.", device.name);

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
        private ICollection<SensorReading> CreateSensorData(SensorDevice device, RawTempData data, TagInfo tag)
        {
            List<SensorReading> readings = new List<SensorReading>();

            if (data.d.Any())
            {

                Utils.Log("Processing {0} data points...", data.d.Count());


                foreach (var datapoint in data.d)
                {
                    int? battPercent = null;

                    // The battery info from the Tag will be for right now (i.e., when we 
                    // queried it). So only use it to fill in the battery percentage for 
                    // data points in the last 24 hours.
                    if (Math.Abs(DateTime.Now.Subtract(datapoint.time).Hours) <= 24)
                    {
                        battPercent = (int)(tag.batteryRemaining * 100);
                    }

                    readings.Add(new SensorReading
                    {
                        timestamp = datapoint.time,
                        device = device,
                        temperature = datapoint.temp_degC,
                        lux = datapoint.lux,
                        humidity = datapoint.cap,
                        battery = datapoint.battery_volts,
                        batteryPercentage = battPercent
                    });
                }
            }

            if (readings.Any())
                Utils.Log("Found {0} readings for device '{1}' (latest: {2:dd-MMM-yy HH:mm:ss}).", readings.Count(), device.name, readings.Max(x => x.timestamp));
            else
                Utils.Log("No readings found in data for device '{0}'.", device.name);
            
            return readings;
        }

        /// <summary>
        /// Stores the sensor readings.
        /// </summary>
        /// <param name="readings">Readings.</param>
        private void StoreReadings<T>(ICollection<T> readings, string indexName) where T : Reading
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

                foreach (var kvp in years)
                {
                    if (kvp.values.Any())
                    {
                        string yearIndex = string.Format("{0}-{1}", indexName, kvp.year);

                        InsertInYearBatches(kvp.values, yearIndex);
                    }
                }

                EsClient.createDateBasedAliases(indexName);
            }
            else
                Utils.Log("No readings to ingest.");
        }

        /// <summary>
        /// Split the list into sub-lists of batch size, and insert
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="items"></param>
        /// <param name="batchSize"></param>
        /// <param name="index"></param>
        private void InsertInYearBatches<T>(List<T> items, string index ) where T : Reading
        {
            var now = DateTime.UtcNow;

            foreach (var x in items)
                x.ingestionTimeStamp = now;

            Utils.Log($"Bulk inserting {items.Count()} for {index}...");
            EsClient.BulkInsert(index, items);
        }

        /// <summary>
        /// Check the battery for either low voltage or low percentage. Either
        /// can trigger a low battery alert.
        /// </summary>
        /// <param name="reading"></param>
        /// <param name="settings"></param>
        /// <returns></returns>
        private static bool DeviceHasLowBattery( SensorReading reading, Settings settings )
        {
            bool lowBattery = false;

            // First check the voltage
            if(reading.battery.HasValue )
            {
                // Check for low battery by voltagee
                if ( reading.device.type != "HiveHome" && reading.battery > 0.0 )
                {
                    lowBattery = reading.battery < settings.lowBatteryThresholdVolts;
                }
            }

            // Now check the percentage.
            if( ! lowBattery && reading.batteryPercentage.HasValue )
            {
                // Check for low battery via percentage
                if (reading.batteryPercentage < settings.lowBatteryThresholdPercent)
                    lowBattery = true;
            }

            return lowBattery;
        }

        /// <summary>
        /// Analyses the battery levels for all devices and pulls out the lowest battery 
        /// reading in the batch of sensor readings. If any devices have a battery level
        /// of less than 1.0, an email is sent warning for all devices.
        /// </summary>
        /// <param name="readings">Readings.</param>
        private void CheckBatteryStatus(ICollection<SensorReading> readings, Settings batterySettings, List<Alert> alerts)
        {
            Utils.Log("Checking battery status for devices...");

            var recentBattery = readings.GroupBy(x => x.device)
                                        .Select(x => new {
                                            Name = x.Key.name,
                                            MinBattery = x.Select(y => y.battery).Min(),
                                            MinBatPct = x.Select(y => y.batteryPercentage).Min() })
                                        .ToList();

            recentBattery.ForEach(x => { Utils.Log($"Battery level for {x.Name}: {x.MinBattery:N2}v, {x.MinBatPct}% "); });

            var lowBatteryDevices = readings.Where(x => DeviceHasLowBattery( x, batterySettings ) )
                                            .GroupBy(x => x.device,
                                                     y => y.battery,
                                                     (key, b) => new { device = key, lowestBattery = b.Min() })
                                            .ToList();

            if (lowBatteryDevices.Any())
            {
                foreach (var status in lowBatteryDevices)
                {
                    var lastReading = readings.Max(x => x.timestamp);
                    var msg = $"Low Battery - {status.lowestBattery:N2}v (last reading: {lastReading:dd-MMM-yyyy HH:mm:ss})\n";
                    alerts.Add(new Alert { deviceName = status.device.name, alertText = msg });
                }
            }
        }


        private void DeleteWeatherDateRange()
        {
            var q = new DateRangeQuery
            {
                Name = "named_query",
                Boost = 1.1,
                GreaterThanOrEqualTo = DateTime.Now.AddDays(-14),
                LessThanOrEqualTo = DateTime.Now,
                Format = "dd/MM/yyyy||yyyy"
            };


            var allDocs = new List<WeatherSummary>();

            EsClient.ScanAllDocs<WeatherSummary>(settings.weatherUnderground.IndexName, 
                                                     "timestamp", (doc, id) => { allDocs.Add(new WeatherSummary { Id = id }); }, q);

            var bulkResponse = EsClient.DeleteMany( allDocs );

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
            var alerts = new List<Alert>();
            var allReadings = new List<SensorReading>();
            var allDevices = new List<SensorDevice>();

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
                    else
                        alerts.Add(new Alert { deviceName = "Wireless Tags", alertText = "Sign-in failed." });
                }
            }
            catch (Exception ex)
            {
                Utils.Log("Exception querying SensorTag data. {0}", ex);
            }

            QueryHiveData(allReadings, alerts);
            QueryWeatherData(settings.weatherUnderground);

            if (allReadings.Any())
            {
                try
                {
                    StoreReadings(allReadings, settings.indexname);
                }
                catch (Exception ex)
                {
                    Utils.Log("Exception ingesting data in ES. {0}", ex);
                }
            
                // See if any of the data we got back indicated a drained battery.
                CheckBatteryStatus(allReadings, settings, alerts);
            }

            try
            {
                // Now check for any missing data - i.e., long gaps since we last saw anything
                CheckMissingData(allDevices, alerts);

                // And send any alerts we saw.
                if (alerts.Any())
                {
                    Utils.Log("Sending {0} alerts.", alerts.Count);

                    if (settings.email != null)
                        Utils.SendAlertEmail(settings.email, alerts);
                }
            }
            catch (Exception ex)
            {
                Utils.Log("Exception checking missing data. {0}", ex);
            }

            Utils.Log("Run complete.");
        }

        private void QueryHiveData(List<SensorReading> readings, List<Alert> alerts )
        {
            try
            {
                if (settings.hive != null)
                {
                    HiveService service = new HiveService();

                    if (service.SignIn(settings.hive.username, settings.hive.password))
                    {

                        var hiveReadings = QueryHiveData(service);

                        readings.AddRange(hiveReadings);
                    }
                    else
                        alerts.Add(new Alert { deviceName = "Hive", alertText = "Sign-in failed." });
                }
            }
            catch (Exception ex)
            {
                Utils.Log("Exception querying Hive data. {0}", ex);
            }
        }

        private void QueryWeatherData(WUGSettings weatherSettings)
        {
            try
            {
                WeatherService weather = new WeatherService(weatherSettings);
                var readings = new List<WeatherReading>();
                var summaries = new List<WeatherSummary>();

                var watermark = getWeatherHighWaterMark();
                var today = DateTime.Now;
                int totalCalls = 0;

                var mostRecent = watermark;

                while (mostRecent < today && totalCalls < 200)
                {
                    Utils.Log("Querying weather for {0}", mostRecent);

                    if (!weather.GetHistory(mostRecent, readings, summaries))
                        break;

                    totalCalls++;

                    mostRecent = mostRecent.AddDays(1);

                    // Max of 10 weather calls per minute
                    Thread.Sleep(6 * 1000);

                    if (totalCalls % 25 == 0)
                    {
                        StoreWeatherReadings(watermark, readings, summaries);
                    }
                }

                StoreWeatherReadings(watermark, readings, summaries);
            }
            catch( Exception ex )
            {
                Utils.Log("Exception querying Weather data. {0}", ex);
            }
        }

        private void StoreWeatherReadings( DateTime watermark, IList<WeatherReading> readings, IList<WeatherSummary> summaries )
        {
            var filteredReadings = readings.Where(x => x.timestamp > watermark).ToList();
            var filteredSummaries = summaries.Where(x => x.timestamp > watermark).ToList();

            StoreReadings<WeatherReading>(filteredReadings, settings.weatherUnderground.IndexName); 
            StoreReadings<WeatherSummary>(filteredSummaries, settings.weatherUnderground.SummaryIndexName);

            readings.Clear();
            summaries.Clear();
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
                    alerts.Add(new Alert { deviceName = device.name, alertText = $"No data for {minsSinceLastReading} minutes." });
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

                LogHandler.InitLogs( settings.logLocation );
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
                Console.WriteLine($"Unable to initialise settings: {ex.Message}");              
            }
        }
    }
}
