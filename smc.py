# Author: Matthew Povey - matt.povey@gmail.com github.com/mattpovey
# Date: 2023-03-07
# Script to read smartmeter data from a P1 port of the ISKRA AM550 smartmeter.
# Data is pushed to influxDB for storage and visualisation.
# Additional smartmeters can be supported but will likely require a different
# telegram specification defined in sme_readings. 

# -- dependencies
# Requires the dsmr_parser library from https://github.com/ndokter/dsmr_parser
# Requires access to an InfluxDB server or InfluxDB cloud instance
# Optionally requires access to a Grafana server for visualisation though basic
# visualisation is available in InfluxDB

from dsmr_parser import telegram_specifications
# This import should be updated for meter type. See the dsmr_parser github
from dsmr_parser.clients import SerialReader, SERIAL_SETTINGS_V5
import os, os.path 
import time
import time
import influxdb_client
import sys
import syslog
from influxdb_client.client.write_api import SYNCHRONOUS
import configparser

# Import configuration
# Read configuration file
config = configparser.ConfigParser()
config.read('smc.ini')

# -----------------------------------------------------------------------------
# Uses the current telegram (does not need to be passed), tagset and the 
# name of a buffer.
# The sm_idbprep() function is called to obtain the correct timestamp
# and equipment ID for the current telegram.
# The line protocol for this telegram is returned. 
# -----------------------------------------------------------------------------

def record_readings(tagset, lp_buffer, telegram):
    sm_ts, sm_gasts, equipment, gas_equipment = sm_idbprep(telegram)
    lp_batch = []

    for tag_key in tagset:
        this_tag_key = tag_key
        for tag_val in tagset[tag_key].keys():
            this_tag_val = tag_val
            for measurement in tagset[tag_key][tag_val].keys():
                msr_request = measurement
                msr_record = tagset[tag_key][tag_val][measurement]
                this_value = str(getattr(telegram, msr_request).value)
                this_unit = str(getattr(telegram, msr_request).unit)
                if this_unit == "None":
                    this_unit = "Count"
                if this_tag_val == "Gas":
                    equipment = gas_equipment
                fields = this_unit + "=" + this_value
                tag_set = "MeterID" + "=" + equipment + "," + this_tag_key + "=" + this_tag_val
                lp_out = msr_record + "," + tag_set + " " + fields + " " + sm_ts
                lp_batch.append(lp_out)

    return lp_batch


# -----------------------------------------------------------------------------
# Take a line or batch of InfluxDB line protocol and push it to the 
# InfluxDB server.
# -----------------------------------------------------------------------------
def push2idb(lp_accumulator, lp_buffer, telegram):
    # Server metadata
    # Note that https:// requires a cert if using local CA
    client = influxdb_client.InfluxDBClient(
        bucket=config['InfluxDB']['bucket'],
        url=config['InfluxDB']['url'],
        token=config['InfluxDB']['token'],
        org=config['InfluxDB']['org']
    )

    # Try to write the data to InfluxDB.
    # If the write fails, write the line protocol to the buffer file for later
    # processing and continue.
    for lp_out in lp_accumulator:
        try:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            write_api.write(url=config['InfluxDB']['url'], bucket=config['InfluxDB']['bucket'], org=config['InfluxDB']['org'], record=lp_out)
            # print(f"Successfully wrote {lp_out} to InfluxDB.")
        except Exception as e:
            error = "Failed to write to InfluxDB: " + str(e)
            lp_buffer.write(lp_out)
            lp_buffer.write("\n")
            logMsg(error, exit=False)
    logMsg(f"Wrote {len(lp_accumulator)} records to InfluxDB bucket, {config['InfluxDB']['bucket']}.")
    return len(lp_accumulator)

# -----------------------------------------------------------------------------
# Configure the listener and return the serial reader object
# -----------------------------------------------------------------------------
def p1_listener():
    # It is probably not useful to test whether the port is in use since the dsmr
    # library does not seem to hold a connection. It will only report that the 
    # port is in use if it happens to be sending data at that time. 

    try:
        # Update for serial port configuration and meter type
        # See imports for serial_settings and telegram_specifications
        serial_reader = SerialReader(
            device = '/dev/cuaU0',
            serial_settings = SERIAL_SETTINGS_V5,
            telegram_specification = telegram_specifications.V5
        )

    except Exception as e:
        error = "Failed to open serial port.", str(e)
        print(error)
        logMsg(error, exit=True)
    
    print(f"Created serial object with the following options: {serial_reader}")

    try:
        print("Testing serial connection to smartmeter.")
        for telegram in serial_reader.read_as_object():
            print(str(getattr(telegram, "P1_MESSAGE_TIMESTAMP").value))
            break
    except Exception as e:
        error = "Could not open serial port" + str(e)
        logMsg(error, exit=True)
    finally:
        print("P1 serial connection to smartmeter is available.")
        return serial_reader

# -----------------------------------------------------------------------------
# sm_idbprep() - Prepare the data for InfluxDB
# Configures the tagset (InfluxDB schema equivalent) and returns four values
# that are used to generate the line protocol output
# DSMR_Parser turns the telegram into an object with properties that can be
# accessed by name.
# -----------------------------------------------------------------------------
def sm_idbprep(telegram):

    # The Gas timestamp comes from the HOURLY_GAS_METER_READING.datetime property
    # Metadata to select readings
    elec_equip_id = "EQUIPMENT_IDENTIFIER"
    gas_equip_id = "EQUIPMENT_IDENTIFIER_GAS"
    elec_ts = "P1_MESSAGE_TIMESTAMP"
    gas_ts = "HOURLY_GAS_METER_READING"

    # Get the time for this telegram and format it to iso
    # Note that electricity and gas have different ts due to gas only being 
    # collected each hour

    # Adding 2 hours to the timestamps to account for the clock on the meter
    # being wrong. This may break when SummerTime ends...
    # 20230328 - Not sure if it's the meter that's in Amsterdam time or 
    # the meter reader. Either way, this makes the timestamp UTC.

    sm_ts = (str(getattr(telegram, elec_ts).value))
    sm_ts = time.strptime(sm_ts, '%Y-%m-%d %H:%M:%S%z') 
    sm_ts = str(time.mktime(sm_ts) + 6400 )[:-2] + "000000000"

    sm_gasts = str(getattr(telegram, gas_ts).datetime)
    sm_gasts = time.strptime(sm_gasts, '%Y-%m-%d %H:%M:%S%z')
    sm_gasts = str(time.mktime(sm_gasts) + 6400 )[:-2] + "000000000"

    # Breaks for gas currently...
    equipment = str(getattr(telegram, elec_equip_id).value)
    gas_equipment = str(getattr(telegram, gas_equip_id).value)
    
    print("Timestamp of current telegram, fixed by 2hrs is, ", (str(getattr(telegram, elec_ts).value)))
    return sm_ts, sm_gasts, equipment, gas_equipment

# -----------------------------------------------------------------------------
# TAGSET DEFINITIONS FOR INFLUXDB
# -----------------------------------------------------------------------------

test_tags = {
    "Tariff": \
        {"Tariff_1": \
            {"ELECTRICITY_USED_TARIFF_1": "ELECTRICITY_USED_AT_TARIFF",
            "ELECTRICITY_DELIVERED_TARIFF_1": "ELECTRICITY_DELIVERED_AT_TARIFF"
            },
        "Tariff_2": \
            {"ELECTRICITY_USED_TARIFF_2": "ELECTRICITY_USED_AT_TARIFF",
            "ELECTRICITY_DELIVERED_TARIFF_2": "ELECTRICITY_DELIVERED_AT_TARIFF"
            },
        }
}

sme_readings = {    
    # Top level tag is key to a further dict
    # Second level key is used to access data
    # Second level value is used to name it
    "Phase": \
        {"Phase_L1": \
            {"VOLTAGE_SAG_L1_COUNT": "VOLTAGE_SAG",
            "VOLTAGE_SWELL_L1_COUNT": "VOLTAGE_SWELL",
            "INSTANTANEOUS_VOLTAGE_L1": "INSTANTANEOUS_VOLTAGE",
            "INSTANTANEOUS_CURRENT_L1": "INSTANTANEOUS_CURRENT",
            "INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE": "INSTANTANEOUS_ACTIVE_POWER_POSITIVE",
            },
        "Phase_L2": \
            {"VOLTAGE_SAG_L2_COUNT": "VOLTAGE_SAG",
            "VOLTAGE_SWELL_L2_COUNT": "VOLTAGE_SWELL",
            "INSTANTANEOUS_VOLTAGE_L2": "INSTANTANEOUS_VOLTAGE",
            "INSTANTANEOUS_CURRENT_L2": "INSTANTANEOUS_CURRENT",
            "INSTANTANEOUS_ACTIVE_POWER_L2_POSITIVE": "INSTANTANEOUS_ACTIVE_POWER_POSITIVE"
            },
        "Phase_L3": \
            {"VOLTAGE_SAG_L3_COUNT": "VOLTAGE_SAG",
            "VOLTAGE_SWELL_L3_COUNT": "VOLTAGE_SWELL",
            "INSTANTANEOUS_VOLTAGE_L3": "INSTANTANEOUS_VOLTAGE",
            "INSTANTANEOUS_CURRENT_L3": "INSTANTANEOUS_CURRENT",
            "INSTANTANEOUS_ACTIVE_POWER_L3_POSITIVE": "INSTANTANEOUS_ACTIVE_POWER_POSITIVE"
            },
        "Phases_L123": \
            {"CURRENT_ELECTRICITY_USAGE": "TOTAL_ACTIVE_POWER",
            "ELECTRICITY_ACTIVE_TARIFF": "ACTIVE_TARIFF", 
            "LONG_POWER_FAILURE_COUNT": "LONG_POWER_FAILURE_COUNT", 
            "SHORT_POWER_FAILURE_COUNT": "SHORT_POWER_FAILURE_COUNT"
            },
        },
    "Tariff": \
        {"Tariff_1": \
            {"ELECTRICITY_USED_TARIFF_1": "ELECTRICITY_USED_AT_TARIFF",
            "ELECTRICITY_DELIVERED_TARIFF_1": "ELECTRICITY_DELIVERED_AT_TARIFF"
            },
        "Tariff_2": \
            {"ELECTRICITY_USED_TARIFF_2": "ELECTRICITY_USED_AT_TARIFF",
            "ELECTRICITY_DELIVERED_TARIFF_2": "ELECTRICITY_DELIVERED_AT_TARIFF"
            },
        "Gas": \
            {"HOURLY_GAS_METER_READING": "HOURLY_GAS_METER_READING",
            }
        }
}

# Logging
def logMsg(message, exit=False):
    syslog.openlog(logoption=syslog.LOG_PID, facility=syslog.LOG_DAEMON)
    syslog.syslog(message)
    print(message)
    if exit:
        syslog.syslog("sm-collector.py exiting.")
        print("sm-collector.py exiting.")
        sys.exit()

def main():
    # Attempt to open lp_buffer.json
    # If it exists, leave the file handle open, otherwise, create it and write
    # the error to syslog
    # TODO: Need a sensible way of pushing accumulated buffer data to InfluxDB
    print("Smartmeter reader starting.")
    buffer_file = "/var/db/lp_buffer.json"

    try:
        print("Testing for buffer file to log data if InfluxDB server is \
unavailable.")
        lp_buffer = open(buffer_file, "a+")
    except Exception as e:
        error = "Unable to open " + buffer_file + " for writing. Check \
permissions." + str(e)
        logMsg(error, exit=True)

    # Create the serial port object
    try:
        print("Creating serial port object.")
        serial_reader = p1_listener()
    except Exception as e:
        error = "Unable to create serial port object. Check permissions \
and that it is the correct port for this OS." + str(e)
        logMsg(error, exit=True)

    print("Serial port object created.")

    # Loop forever reading telegrams 
    # Every 1000 telegrams, write a lot entry to syslog with the time and number
    # of telegrams read
    BATCH_SIZE = 10
    tel_count = 0
    lp_accumulator = []

    try:
        for telegram in serial_reader.read_as_object():
            if tel_count % 10000 == 0:
                progress_message = "Read " + str(tel_count) + " telegrams to Influxdb since startup"
                syslog.syslog(syslog.LOG_INFO, progress_message)

            lp_accumulator.extend([record_readings(sme_readings, lp_buffer, telegram)])
            tel_count += 1

            if len(lp_accumulator) >= BATCH_SIZE:
                try:
                    num_pushed = push2idb(lp_accumulator, lp_buffer, telegram)
                    # print("Pushed " + str(num_pushed) + " records to InfluxDB.")
                    lp_accumulator.clear()
                except Exception as e:
                    error = "Error pushing data to InfluxDB: " + str(e)
                    logMsg(error, exit=False)
                    print("Continuing data collection.")
    except Exception as e:
        error = "Error reading telegram: " + str(e)
        logMsg(error, exit=True)
    except KeyboardInterrupt:
        print("Keyboard interrupt. Exiting.")
        sys.exit()

if __name__ == "__main__":
    main()
