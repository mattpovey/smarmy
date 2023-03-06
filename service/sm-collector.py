from dsmr_parser import telegram_specifications
from dsmr_parser.clients import SerialReader, SERIAL_SETTINGS_V5
import os, os.path 
import datetime
import time
import influxdb_client
import sys
import syslog
from influxdb_client.client.write_api import SYNCHRONOUS


# -----------------------------------------------------------------------------
# FUNCTION DEFINITIONS
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Uses the current telegram (does not need to be passed), tagset and a location 
# for a buffer file.
# The sm_idbprep() function is called to obtain the correct timestamp
# and equipment ID for the current telegram.
# The line protocol is then passed to push2idb() to be written to InfluxDB
# -----------------------------------------------------------------------------

def record_readings(tagset, lp_buffer):
    #os.system('clear')
    sm_ts, sm_gasts, equipment, gas_equipment = sm_idbprep()

    # Iterate through the tagset getting the data from the current telegram
    # for each measurement and iteratively building the line protocol output
    for tag_key in tagset:
        this_tag_key = tag_key
        for tag_val in tagset[tag_key].keys():
            this_tag_val = tag_val
            for measurement in tagset[tag_key][tag_val].keys():
                # The field name used to identify the value in the telegram
                msr_request = measurement
                # The field name that will be used in InfluxDB
                msr_record = tagset[tag_key][tag_val][measurement]       
                this_value = str(getattr(telegram, msr_request).value) 
                this_unit = str(getattr(telegram, msr_request).unit)
                # Fix up units - the "none" unit is used for counts
                if this_unit == "None":
                    this_unit = "Count"
                # Capture the gas meter number
                if this_tag_val == "Gas":
                    equipment = gas_equipment
                fields = this_unit + "=" + this_value
                # Build the line protocol output
                tag_set = "MeterID" + "=" + equipment + "," + this_tag_key + "=" + this_tag_val
                lp_out = msr_record + "," + tag_set + " " + fields + " " + sm_ts
                #print(lp_out)
                print("LP Prepared...")

                # Attempt to push the line protocol to InfluxDB
                # If push2idb returns False, write the line to lp_buffer and log
                # the failure to syslog
                # TODO: Batch this using write_points and a buffer array
                # TODO: The whole array can be dumped to the buffer file it it fails

                #print("Attempting push to InfluxDB...")
                if push2idb(lp_out) == False:
                    lp_buffer.write(lp_out)
                    lp_buffer.write("\n")
                    syslog.syslog(syslog.LOG_ERR, "Failed to write to InfluxDB")
                    #syslog.syslog(syslog.LOG_ERR, lp_out)
                    print("InfluxDB write failed")
                #else:
                    #print("InfluxDB write successful")

# -----------------------------------------------------------------------------
# Take a line of InfluxDB line protocol and push it to the InfluxDB server.
# Return True if successful, False if not.
# TODO: Report error if the server is not available.
# -----------------------------------------------------------------------------
def push2idb(lp_out):
    # Server metadata
    # Note that https:// requires a cert if using local CA
    # TODO: Move this to a config file
    bucket = "sm_collector"
    url = "https://influxdb.sys.kyomu.co.uk:8086"
    org = "kyomu.co.uk"
    token = "!!28AWec8baj88R0Do-92VevegExVRDEfs7vQm_Y9xVA4GutIbjcAevmTUVRp3OqrDZWY7SunrFD31-oDqHFvm3A=="
    client = influxdb_client.InfluxDBClient(
        url=url,
        token=token,
        org=org
    )

    # Try to write the data to InfluxDB.
    # Return True if successful. If the write fails, try to log the error and
    # return False
    try:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(url=url, bucket=bucket, org=org, record=[lp_out])
        return True
    except Exception as e:
        syslog.syslog(syslog.LOG_ERR, "Failed to write to InfluxDB", + str(e))
        return False
    
def p1_listener():
    # It is probably not useful to test whether the port is in use since the dsmr
    # library does not seem to hold a connection. It will only report that the 
    # port is in use if it happens to be sending data at that time. 
    try:
        serial_reader = SerialReader(
            # TODO: Move this to a config file
            #device='/dev/ttyUSB0',
            device='/dev/cuaU0',
            serial_settings=SERIAL_SETTINGS_V5,
            telegram_specification=telegram_specifications.V5
        )
    except Exception as e:
        syslog.syslog(syslog.LOG_ERR, "Failed to open serial port.", + str(e))
        print("Failed to open serial port", + str(e))
        sys.exit()

    try:
        for telegram in serial_reader.read_as_object():
            print(str(getattr(telegram, "P1_MESSAGE_TIMESTAMP").value))
            break
    except:
        print("Device reports readiness to read but returned no data. \
              \nCheck whether the port is already in use.")
        sys.exit()
    finally:
        print("P1 serial connection to smartmeter is avaialble.")
        return serial_reader

# -----------------------------------------------------------------------------
# sm_idbprep() - Prepare the data for InfluxDB
# Configures the tagset (InfluxDB schema equivalent) and returns four values
# that are used in the line protocol output
# -----------------------------------------------------------------------------
def sm_idbprep():

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
    # being wrong. This will break when SummerTime ends...

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

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

# Attempt to open lp_buffer.json
# If it exists, leave the file handle open, otherwise, create it and write
# the error to syslog
print("Smartmeter reader starting.")
buffer_file = "/var/db/lp_buffer.json"
try:
    print("Testing for buffer file to log data if InfluxDB server is \
        unavailable.")
    lp_buffer = open(buffer_file, "a+")
except Exception as e:
    syslog.syslog("Unable to open /var/db/lp_buffer.json for writing. \
                    Check permissions." + str(e))
    print("Unable to open /var/db/lp_buffer.json for writing.", + str(e))
    sys.exit()

# Create the serial port object

try:
    print("Creating serial port object.")
    serial_reader = p1_listener()
except:
    syslog.syslog("Unable to create serial port object. Check permissions \
                  and that it is the correct port for this OS.")
    syslog.syslog("sm-collector.py exiting.")
    print("Unable to create serial port object. Check permissions \
                  and that it is the correct port for this OS.")
    print("sm-collector.py exiting.")
    sys.exit()

print("Serial port object created.")

# Loop forever reading telegrams 
# Every 1000 telegrams, write a lot entry to syslog with the time and number
# of telegrams read
tel_count = 0
try:
    for telegram in serial_reader.read_as_object():
        if tel_count % 1000 == 0:
            syslog.syslog("Read " + str(tel_count) + " telegrams to Influxdb since .")
        record_readings(sme_readings, lp_buffer)
        tel_count += 1
except Exception as e:
        syslog.syslog("Error reading telegram: " + str(e))
        syslog.syslog("sm-collector.py exiting.")
        print("Error reading telegram: " + str(e))
        print("sm-collector.py exiting.")
        sys.exit()
