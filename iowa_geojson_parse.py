import json

with open('input\geojson\states_low_rez_gz_2010_us_040_00_20m.json') as raw_state_json:
    raw_state_data = json.loads(raw_state_json.read())

for state in raw_state_data['features']:
    # print(state['properties']['STATE'], state['properties']['NAME'])
    if state['properties']['NAME'] == 'Iowa':
        print(len(state['geometry']['coordinates'][0]))
        iowa_coordinates = state['geometry']['coordinates'][0]
