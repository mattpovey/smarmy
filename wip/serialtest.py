import serial

ser = serial.Serial('/dev/ttyUSB0')
if ser.isOpen:
    print("Port is already open")
else:
    print("Port is available")
