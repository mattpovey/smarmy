import json

with open('/home/mjpadmin/Projects/dsmr/test.json', 'r') as f_json:
    smdata = json.load(f_json)
    print(smdata)
for i in smdata:
    print(i.value)
