from dsmr_parser import telegram_specifications
from dsmr_parser.clients import SerialReader, SERIAL_SETTINGS_V5
from dsmr_parser.objects import CosemObject, MBusObject, Telegram
import os, os.path 
import datetime
import time
import influxdb_client
import sys
from influxdb_client.client.write_api import SYNCHRONOUS

# -----------------------------------------------------------------------------
# FUNCTION DEFINITIONS
# -----------------------------------------------------------------------------

#def record_readings_3(serial_obj, tagset, locallog:bool):
    # Prepares influx line protocol lines for 3 level tag structures


#def record_readings_2(serial_obj, tagset, locallog:bool):
    # Prepares influx line protocol lines for 2 level tag structures

#def record_readings_1(serial_obj, tagset, locallog:bool):
    # Prepares influx line protocol lines for 3 level tag structures

# Takes whatever line of influx line protocol has been prepared as lp_out and 
# pushes it to the influxdb server
def push2idb(lp_out):
    bucket = "smartmeter_test"
    url = "http://127.0.0.1:8086"
    org = "kyomu.co.uk"
    token = "qXlan1P2SOcUx-sAw17WEb_1ONkbao9yJTz3csSjx_ATX9nXxP5Wb58KpOGerV56vuGovtDt5Sc1E_YH6VsCcg=="
    client = influxdb_client.InfluxDBClient(
        url=url,
        token=token,
        org=org
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(url=url, bucket=bucket, org=org, record=[lp_out])

# It is probably not useful to test whether the port is in use since the dsmr
# library does not seem to hold a connection. It will only report that the 
# port is in use if it happens to be sending data at that time. 
def p1_listener():
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



# -----------------------------------------------------------------------------
# TAGSET DEFINITIONS FOR INFLUXDB
# -----------------------------------------------------------------------------

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
        }
}

# These do not have units and are stored as NONE
# Needs to be handled differently as they only receive the meter tag. 
sme_general = {
    "General": \
        {"ELECTRICITY_ACTIVE_TARIFF": "ACTIVE_TARIFF", 
        "LONG_POWER_FAILURE_COUNT": "LONG_POWER_FAILURE_COUNT", 
        "SHORT_POWER_FAILURE_COUNT": "SHORT_POWER_FAILURE_COUNT"
        }
}

smg_readings = {
    "EQUIPMENT_IDENTIFIER_GAS": "HOURLY_GAS_METER_READING",
}


# -----------------------------------------------------------------------------
# VARIABLE DEFINITIONS
# -----------------------------------------------------------------------------

# Note that the EQUIPMENT_IDENTIFIER and P1_MESSAGE_TIMESTAMP are addressed directly
# The Gas timestamp comes from the HOURLY_GAS_METER_READING.datetime property

# Metadata to select readings

elec_equip_id = "EQUIPMENT_IDENTIFIER"
gas_equip_id = "EQUIPMENT_IDENTIFIER_GAS"
elec_ts = "P1_MESSAGE_TIMESTAMP"
gas_ts = "HOURLY_GAS_METER_READING"

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

#serial_reader = p1_listener()
#record_readings(serial_reader, sme_readings, 0)


# -----------------------------------------------------------------------------
# TESTING
# -----------------------------------------------------------------------------



#n = dict_depth(sme_readings) 
#print(n)

def read_keys(dict, n, tlist, flist):
    if n > 1:
        for x in dict.keys():
            #print(x)
            tlist.append(x)
            read_keys(dict[x], n-1, tlist, flist)
    else:
        for x, y in dict.items():
            flist.append(x)
            flist.append(y)
            return tlist


# enables use of the dict_depth() function to prepare the function call. 
# I think I'm just going to have to write one function for 3 level, one for 2 level and one for 1 level.
tags_list = []
field_list = []
tags = read_keys(sme_readings, 3, tags_list, field_list)
print(tags)
print("\n")



#for x in sme_readings.keys():
    #print(x)
    #for y in sme_readings[x].keys():
        #print(y)
        #for z in sme_readings[x][y].keys():
            #print(z)
            #for a in sme_readings[x][y][z].keys():
                #print(a)

#for tag_key in sme_readings:
    #print(dir(sme_readings[tag_key]))
    #print("Tag_key: " + tag_key)
    #print(sme_readings[tag_key])

    #for tag_value in sme_readings[tag_key]:
        #if type(sme_readings[tag_key][tag_value]) == dict:
            #print("Hello")
            #break
        #print(sme_readings[tag_key][tag_value])
            #print("Tag: " + tag_value)
            #print(sme_readings[tag_key][tag_value])
            #for measurement in sme_readings[tag_key][tag_value]:
                #print(sme_readings[tag_key][tag_value][measurement])
            #print("\n")

                #print("Measurement: " + measurement + " will be stored as: " + sme_readings[tag_key][tag_value][measurement])
    #else:
        #break
    