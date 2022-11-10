from multiprocessing.sharedctypes import Value
from dsmr_parser import telegram_specifications
from dsmr_parser.clients import SerialReader, SERIAL_SETTINGS_V5
from dsmr_parser.objects import CosemObject, MBusObject, Telegram
import os

serial_reader = SerialReader(
    device='/dev/ttyUSB0',
    serial_settings=SERIAL_SETTINGS_V5,
    telegram_specification=telegram_specifications.V5
)

# Create lists for the electricity (sme) and gas (smg) fields
# Store these separately as they have different time stamps 
# due to gas only being recorded hourly.
#sme_tags = \
    #["EQUIPMENT_IDENTIFIER",
    #"TARIFF_1",


sme_fields = \
    ["P1_MESSAGE_TIMESTAMP",
    
    "ELECTRICITY_USED_TARIFF_1", 
    "ELECTRICITY_USED_TARIFF_2", 
    "ELECTRICITY_DELIVERED_TARIFF_1", 
    "ELECTRICITY_DELIVERED_TARIFF_2",
    "ELECTRICITY_ACTIVE_TARIFF", 
    "CURRENT_ELECTRICITY_USAGE", 
    "LONG_POWER_FAILURE_COUNT", 
    "SHORT_POWER_FAILURE_COUNT", 
    "VOLTAGE_SAG_L1_COUNT", 
    "VOLTAGE_SAG_L2_COUNT", 
    "VOLTAGE_SAG_L3_COUNT", 
    "VOLTAGE_SWELL_L1_COUNT", 
    "VOLTAGE_SWELL_L2_COUNT", 
    "VOLTAGE_SWELL_L3_COUNT", 
    "INSTANTANEOUS_VOLTAGE_L1", 
    "INSTANTANEOUS_VOLTAGE_L2", 
    "INSTANTANEOUS_VOLTAGE_L3", 
    "INSTANTANEOUS_CURRENT_L1", 
    "INSTANTANEOUS_CURRENT_L2", 
    "INSTANTANEOUS_CURRENT_L3", 
    "INSTANTANEOUS_ACTIVE_POWER_L1_POSITIVE", 
    "INSTANTANEOUS_ACTIVE_POWER_L2_POSITIVE", 
    "INSTANTANEOUS_ACTIVE_POWER_L3_POSITIVE",]

smg_fields = \
    ["EQUIPMENT_IDENTIFIER_GAS", # tag
    "HOURLY_GAS_METER_READING"]


for telegram in serial_reader.read_as_object():
    os.system('clear')
    # First the electricity readings
    print(" ------------------------- \n Electricity Readings \n ------------------------- \n")
    for e_field in sme_fields:
        sme_value = str(getattr(telegram, e_field).value)  # see 'Telegram object' docs below
        sme_unit = str(getattr(telegram, e_field).unit)
        print(e_field + ", " + sme_value + ", " + sme_unit)
    
    # Then the gas readings
    print("\n ------------------------- \n Gas Readings \n ------------------------- \n")

    for g_field in smg_fields: 
        # additionally grab the timestamp of last gas meter reading
        smg_value = str(getattr(telegram, g_field).value)  # see 'Telegram object' docs below
        smg_unit = str(getattr(telegram, g_field).unit)
        if g_field == "HOURLY_GAS_METER_READING":
            smg_datetime = str(getattr(telegram, g_field).datetime)
            print(g_field + ", " + smg_value + ", " + smg_unit, ", " + smg_datetime)
        else:
            print(g_field + ", " + smg_value + ", " + smg_unit)