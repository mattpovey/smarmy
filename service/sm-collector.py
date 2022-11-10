from dsmr_parser import telegram_specifications
from dsmr_parser.clients import SerialReader, SERIAL_SETTINGS_V5
import os, os.path 
import datetime
import time
import influxdb_client
import sys
from influxdb_client.client.write_api import SYNCHRONOUS

# -----------------------------------------------------------------------------
# FUNCTION DEFINITIONS
# -----------------------------------------------------------------------------

def record_readings(serial_obj, tagset):
    #os.system('clear')
    sm_ts, sm_gasts, equipment, gas_equipment = sm_idbprep()
    ilp_list = []
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
                # Fix up units
                if this_unit == "None":
                    this_unit = "Count"
                # Capture the gas meter number
                if this_tag_val == "Gas":
                    equipment = gas_equipment
                fields = this_unit + "=" + this_value
                # Build the line protocol output
                tag_set = "MeterID" + "=" + equipment + "," + this_tag_key + "=" + this_tag_val
                outline = msr_record + "," + tag_set + " " + fields + " " + sm_ts
                #print(outline)
                push2idb(outline)

def push2idb(lp_out):
    # Takes whatever line of influx line protocol has been prepared as lp_out and 
    # pushes it to the influxdb server
    bucket = "sm_collector"
    url = "http://127.0.0.1:8086"
    org = "kyomu.co.uk"
    token = "nCZ83rEOWe1Cp2rkH3N9GkS5rpOuyfNy8StfXhjDYfFxmY6BB84ofkFWw-4_BD3D2iAzzoM76KpvFzs2ubSWZA=="
    client = influxdb_client.InfluxDBClient(
        url=url,
        token=token,
        org=org
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(url=url, bucket=bucket, org=org, record=[lp_out])


def p1_listener():
    # It is probably not useful to test whether the port is in use since the dsmr
    # library does not seem to hold a connection. It will only report that the 
    # port is in use if it happens to be sending data at that time. 
    serial_reader = SerialReader(
        device='/dev/ttyUSB0',
        serial_settings=SERIAL_SETTINGS_V5,
        telegram_specification=telegram_specifications.V5
    )

    try:
        for telegram in serial_reader.read_as_object():
            print(str(getattr(telegram, "P1_MESSAGE_TIMESTAMP").value))
            break
    except:
        print("Device reports readiness to read but returned no data. \nCheck whether the port is already in use.")
        sys.exit()
    finally:
        print("P1 serial connection to smartmeter is avaialble.")
        return serial_reader

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

serial_reader = p1_listener()
for telegram in serial_reader.read_as_object():
    record_readings(serial_reader, sme_readings)

