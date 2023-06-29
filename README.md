# SmartMeter InfluxDB Collector

Script to read data from a P1 port of the ISKRA AM550 smartmeter. Data from the smartmeter is formatted before being pushed to InfluxDB for storage and visualization. This script can be expanded to support additional smartmeters but will require a different telegram specification to be defined in `sme_readings`. 

## Requirements

- Python 3.9 - 3.11
- [dsmr_parser](https://github.com/ndokter/dsmr_parser) library by [Nigel Dokter](https://github.com/ndokter).
- Access to an InfluxDB server or InfluxDB cloud instance
- (Optional) Access to a Grafana server for visualization, however, basic visualization is available in InfluxDB.

## InfluxDB Setup

This script supports both a local InfluxDB instance or the InfluxDB SaaS. To get started with InfluxDB, follow the instructions on their [official documentation](https://docs.influxdata.com/influxdb/v2.0/get-started/).

The script requires the following information from your InfluxDB instance:

- URL (e.g., localhost:8086 for a local instance, or the URL of your cloud instance)
- Organization
- Token
- Bucket

This information needs to be provided in a configuration file `smc.ini` that should be present in the same directory as this script. 

## Configuration File `smc.ini`

The configuration file `smc.ini` contains the InfluxDB information and is structured as follows:

```ini
[InfluxDB]
url = <Your_InfluxDB_URL>
token = <Your_InfluxDB_Token>
org = <Your_InfluxDB_Org>
bucket = <Your_InfluxDB_Bucket>
```

Replace `<Your_InfluxDB_URL>`, `<Your_InfluxDB_Token>`, `<Your_InfluxDB_Org>`, and `<Your_InfluxDB_Bucket>` with your actual InfluxDB details.

## Serial configuration

Update the following with settings appropriate for your situation. For details of serial_settings and telegram_specification, see the documentation for [dsmr_parser](https://github.com/ndokter/dsmr_parser): 

```
        # Update for serial port configuration and meter type
        # See imports for serial_settings and telegram_specifications
        serial_reader = SerialReader(
            device = '/dev/cuaU0',
            serial_settings = SERIAL_SETTINGS_V5,
            telegram_specification = telegram_specifications.V5
        )
```

## Running the Script

The script can be run interactively and provides feedback on stdout. It is better run as a service. I run it as a daemon on OpenBSD with the following rc script:

```
#!/bin/ksh

# $OpenBSD$

# PROVIDE: smcollector
# REQUIRE: DAEMON
# KEYWORD: shutdown

daemon="/usr/local/bin/python3 /var/smartmeter/sm-collector.py"
daemon_user="_smcollector"
daemon_logger="daemon.info"
daemon_flags=""
pidfile="/var/run/sm-collector.pid"

. /etc/rc.d/rc.subr

rc_bg=YES

rc_cmd $1
```
The script writes an entry to syslog every 
## Notes

- This script uses the dsmr_parser library to parse the smartmeter data. Ensure that the correct telegram specification for your meter is imported.
- The script also requires permissions to access the serial port specified in the `p1_listener()` function.
- The InfluxDB data schema is defined in the `sme_readings` dictionary. 
- There is some oddness with timestamps that is dealt with using a 2hr adjustment to get to UTC. 
- If the script cannot write to InfluxDB, it will write the data to a buffer file `lp_buffer.json`, which can be found at `/var/db/lp_buffer.json`. Code to push this to Influxdb needs to be written but should be straightforward. 

