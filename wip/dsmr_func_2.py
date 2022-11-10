from cgi import FieldStorage
from multiprocessing.sharedctypes import Value
from dsmr_parser import telegram_specifications
from dsmr_parser.clients import SerialReader, SERIAL_SETTINGS_V5
from dsmr_parser.objects import CosemObject, MBusObject, Telegram
import os, os.path 
import datetime
import time
import influxdb_client
import sys
from influxdb_client.client.write_api import SYNCHRONOUS

# Create lists for the electricity (sme) and gas (smg) fields

sme_tags_tariff = {
    "Tariff_1": ["ELECTRICITY_USED_TARIFF_1", "ELECTRICITY_DELIVERED_TARIFF_1"],
    "Tariff_2": ["ELECTRICITY_USED_TARIFF_2", "ELECTRICITY_DELIVERED_TARIFF_2"],
} 

sme_tags_phase = {    
    "Phase_L1": \
        ["VOLTAGE_SAG_L1_COUNT", 
        "VOLTAGE_SWELL_L1_COUNT", 
        "INSTANTANEOUS_VOLTAGE_L1", 
        "INSTANTANEOUS_CURRENT_L1",
        "INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE"
        ],
    "Phase_L2": \
        ["VOLTAGE_SAG_L2_COUNT", 
        "VOLTAGE_SWELL_L2_COUNT", 
        "INSTANTANEOUS_VOLTAGE_L2", 
        "INSTANTANEOUS_CURRENT_L2",
        "INSTANTANEOUS_ACTIVE_POWER_L2_POSITIVE",
        ],
    "Phase_L3": \
        ["VOLTAGE_SAG_L3_COUNT", 
        "VOLTAGE_SWELL_L3_COUNT",  
        "INSTANTANEOUS_VOLTAGE_L3", 
        "INSTANTANEOUS_CURRENT_L3",
        "INSTANTANEOUS_ACTIVE_POWER_L3_POSITIVE",
        ]
}

# Note that the EQUIPMENT_IDENTIFIER and P1_MESSAGE_TIMESTAMP are addressed directly
# The Gas timestamp comes from the HOURLY_GAS_METER_READING.datetime property

elec_equip_id = "EQUIPMENT_IDENTIFIER"
gas_equip_id = "EQUIPMENT_IDENTIFIER_GAS"
elec_ts = "P1_MESSAGE_TIMESTAMP"
gas_ts = "HOURLY_GAS_METER_READING"

sme_tags_aggs = {
    "Aggregates": \
        ["ELECTRICITY_ACTIVE_TARIFF", 
        "CURRENT_ELECTRICITY_USAGE", 
        "LONG_POWER_FAILURE_COUNT", 
        "SHORT_POWER_FAILURE_COUNT",
        ]
}

smg_tags = {
    "EQUIPMENT_IDENTIFIER_GAS": ["HOURLY_GAS_METER_READING"],
}

def sm_idbprep():

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

    
    return sm_ts, sm_gasts

def read_tags(taglist, tag_key, log_file, push):

    # Fetch a given set of fields from smartmeter and append to 'file'
    # Function does not create the telegram object and should be 
    # called from a function that does.
    # Set push to 0 if debugging to reduce database writes
    
    for tag in taglist:
        # print("InfluxDB Line Protocol for: " + tag)
        for item in taglist[tag]:
            # print(item)
            sm_value = str(getattr(telegram, item).value) 
            sm_unit = str(getattr(telegram, item).unit)
            if sm_unit == "None":
                sm_unit = "Count"
            smelec_equipment = str(getattr(telegram, elec_equip_id).value)
            smgas_equipment = str(getattr(telegram, gas_equip_id).value)
            with open(log_file, 'a') as outfile: 

                measurement = item + ","
                fields = sm_unit + "=" + sm_value
                if item == "HOURLY_GAS_METER_READING":
                    tag_set = "MeterID" + "=" + smgas_equipment + " "
                    outline = measurement + tag_set + fields + " " + sm_gasts
                else:
                    tag_set = "MeterID" + "=" + smelec_equipment + "," + tag_key + "=" + tag + " "
                    outline = measurement + tag_set + fields + " " + sm_ts

                print(outline)
                outfile.write(outline)
                outfile.write("\n")
                #outline = "\"" + outline + "\""
                if push == 1:
                    push2idb(outline)
                else: 
                    break

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
    #print("Pushed to IDB")

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

log_file = "/home/mjpadmin/Projects/dsmr/functest.out"
numgrams = 0
serial_reader = p1_listener()
for telegram in serial_reader.read_as_object():
    os.system('clear')
    sm_ts, sm_gasts = sm_idbprep()
    print("Recording every tenth measurement (10s)")
    if numgrams == 9:
        print("Recording Telegram")
        read_tags(sme_tags_tariff, "Tariff", log_file, 1)
        read_tags(sme_tags_phase, "Phase", log_file, 1)
        read_tags(sme_tags_aggs, "Aggregates", log_file, 1)
        read_tags(smg_tags, "gas", log_file, 1)
        numgrams = 0
    else:
        print("Seconds: ",  9 - numgrams)
        print("Skipping.")
        numgrams = numgrams + 1






