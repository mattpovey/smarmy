
# Create a virtual serial port pair
from serial.tools import virtual
port_pair = virtual.SerialPortPair()

# Write the captured data to the input port of the virtual serial port pair
port_pair.write(output.encode('utf-8'))

# Connect to the output port of the virtual serial port pair
virtual_serial = serial.serial_for_url(port_pair.outport.name)

# Read the data from the virtual serial port as if it were coming from the real serial port
while True:
    data = virtual_serial.read(virtual_serial.in_waiting)
    if data:
        print(data.decode('utf-8'))
    time.sleep(0.1)


