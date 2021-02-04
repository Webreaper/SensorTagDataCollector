using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Nest;

namespace SensorTagElastic
{
    [DataContract]
    [ElasticsearchType]
    public class SensorDevice
    {
        [DataMember]
        [Keyword]
        public string uuid { get; set; }
        [DataMember]
        public string name { get; set; }
        [DataMember]
        public string type { get; set; }
        [DataMember]
        public string location { get; set; }
    }

    [DataContract]
    public class WUGSettings
    {
        [DataMember]
        public string apiKey { get; set; }
        [DataMember]
        public string Country { get; set; }
        [DataMember]
        public string City { get; set; }
        [DataMember]
        public string IndexName { get; set; }
        [DataMember]
        public string SummaryIndexName { get; set; }
    }

    [DataContract]
    public class EmailSettings
    {
        [DataMember]
        public string smtpserver { get; set; }
        [DataMember]
        public int smtpport { get; set;  }
        [DataMember]
        public string username { get; set; }
        [DataMember]
        public string password { get; set; }
        [DataMember]
        public string toaddress { get; set; }
        [DataMember]    
        public string fromaddress { get; set; }
        [DataMember]
        public string toname { get; set; }
    }
  
    [DataContract]
    public class HiveSettings
    {
        [DataMember]
        public string username { get; set; }
        [DataMember]
        public string password { get; set; }
    }

    [DataContract]
    public class WirelessTagSettings
    {
        [DataMember(Name = "wirelesstagserviceurl")]
        public string WirelessTagServiceUrl { get; set; }
        [DataMember]
        public string username { get; set; }
        [DataMember]
        public string password { get; set; }
    }

    [DataContract]
    public class Settings
    {
        [DataMember]
        public string indexname { get; set; }
        [DataMember]
        public string elasticserver { get; set; }
        [DataMember]
        public int refreshPeriodMins { get; set; }
        [DataMember]
        public double lowBatteryThresholdVolts { get; set; }
        [DataMember]
        public int lowBatteryThresholdPercent { get; set; }
        [DataMember]
        public int noDataWarningMins { get; set; }
        [DataMember]
        public string logLocation { get; set; }
        [DataMember]
        public HiveSettings hive { get; set; }
        [DataMember]
        public WirelessTagSettings wirelesstag { get; set; }
        [DataMember]
        public EmailSettings email { get; set; }
        [DataMember]
        public WUGSettings weatherUnderground { get; set; }
    }
}
