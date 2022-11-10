import os, os.path 
import datetime
import time
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

lp_out = "ELECTRICITY_USED_TARIFF_1,MeterID=4530303434303037343238343837353139,Tariff=Tariff_1 kWh=3430.043 1666984023"
bucket = "smartmeter_test"
url = "http://127.0.0.1:8086"
org = "kyomu.co.uk"
token = "uehWHV1A_CNzsLx-dvVi6hwb2I5bG1qR29JEQ9O6Ud7Xn-fQ_0NIpb-9Tqe1ozTwB5tbbcEcHlmEk9U810hS6g=="
client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
)
write_api = client.write_api(write_options=SYNCHRONOUS)
write_api.write(url=url, bucket=bucket, org=org, record=lp_out, debug=True)
print("Pushed to IDB")

from influxdb_client import InfluxDBClient

address = "http://127.0.0.1:8086"
token = "uehWHV1A_CNzsLx-dvVi6hwb2I5bG1qR29JEQ9O6Ud7Xn-fQ_0NIpb-9Tqe1ozTwB5tbbcEcHlmEk9U810hS6g=="
org = "kyomu.co.uk"

client = InfluxDBClient(url=address, token=token, org=org)
qapi = client.query_api()

q = 'import "influxdata/influxdb/schema"\n\nschema.tag(bucket: "smartmeter_test")'

tables = qapi.query(q)
for table in tables:
     print(table)
     for record in table.records:
         print(record.values)