# Collect data from the serial port for testing
import serial
import time

sport = '/dev/cuaU0'
#sport = '/dev/ttyUSB0'


# Configure the serial port
ser = serial.Serial(sport, baudrate=115200, timeout=1)

# Capture the output from the serial port
output = ''
start_time = time.time()
while True:
    # Read a line from the serial port
    line = ser.readline().decode('utf-8')
    print(line)
    # Add the line to the output
    output += line
    
    # Stop capturing after a 5 seconds
    if time.time() - start_time > 5:
        break


# Save the output to a file or a buffer in memory
with open('dsmr_data.txt', 'w') as f:
    f.write(output) 
