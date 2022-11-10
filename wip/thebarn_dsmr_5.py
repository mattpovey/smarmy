# DSMR v4.2 p1 using dsmr_parser and telegram objects

from time import sleep
from dsmr_parser import telegram_specifications
from dsmr_parser.clients import SerialReader, SERIAL_SETTINGS_V5
from dsmr_parser.objects import CosemObject, MBusObject, Telegram
from dsmr_parser.parsers import TelegramParser
import os

serial_reader = SerialReader(
    device='/dev/ttyUSB0',
    serial_settings=SERIAL_SETTINGS_V5,
    telegram_specification=telegram_specifications.V5
)

# telegram = next(serial_reader.read_as_object())
# print(telegram)


numgrams = 0
for telegram in serial_reader.read_as_object():    
    with open("/home/mjpadmin/Projects/dsmr/thebarn_iskradsmr5.json", "a") as outfile:
        os.system('clear')
        print(telegram)
        print("Seconds: ",  9 - numgrams)
        print(os.getcwd())
        #print(telegram.to_json())
        #json_object=telegram.to_json()
        if numgrams == 9:
            outfile.write(telegram.to_json())
            outfile.write("\n")
            print("Recording Telegram")
            #print(json_object)
            #outfile.close
            numgrams = 0
        else:
            print("Skipping.")
            numgrams = numgrams + 1


