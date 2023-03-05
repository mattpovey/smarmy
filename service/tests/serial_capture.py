# Collect data from the serial port for testing
import serial
import time

# Configure the serial port
ser = serial.Serial('ttyUSB0', baudrate=115200, timeout=1)

# Capture the output from the serial port
output = ''
while True:
    data = ser.read(ser.in_waiting)
    if data:
        output += data.decode('utf-8')
    time.sleep(0.1)

    # Stop capturing after a few seconds
    if time.time() > start_time + 5:
        break

# Save the output to a file or a buffer in memory
with open('dsmr_data.txt', 'w') as f:
    f.write(output) 
