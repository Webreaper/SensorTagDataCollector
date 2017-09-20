# SensorTagDataCollector
## ElasticSearch Ingester for Wireless Sensor Tag Data.

### What does it do?

Wireless Sensor Tags (http://wirelesstag.net) are small sensors which measure humidity, temperature and ambient light level, and transmit this back to a hub to be stored in the cloud. This app uses the WirelessTag API (http://wirelesstag.net/apidoc.html) to query that data, transform it, and ingest it into an ElasticSearch instance for creating dashboards, plotting trends, etc.

As an added bonus, the process can also query the historic temperature of a Hive Home heating system Thermostat, and ingest that data into the ES index too.

### Settings Storage

The service reads the various settings (ES info, WirelessTag username/password) from a json file. It then queries the service, gathering up the data before writing it to the ES cluster/index specified in the settings file. Data is written to month-based indexes, and then an alias created to cover all indexes.

### Running the service

The service is a .Net/C# application, developed with Visual Studio fo Mac. It can be run on any architecture - for example, I run it using Mono, on a linux-based Synology NAS.

### Disclaimer

I accept no liability for any data loss or corruption caused by the use of this application. Your use of this app is entirely at your own risk - please ensure that you have adequate backups before you use this software.


Software (C) Copyright 2017-2018 Mark Otway
