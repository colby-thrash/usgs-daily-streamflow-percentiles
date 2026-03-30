from streamflow_percentiles.functions import (
    activate_usgs_api_key)

activate_usgs_api_key()

with open(r'\data\test.txt', 'w') as f:
    f.write('test1')
    f.write('test2')