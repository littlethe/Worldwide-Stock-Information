import csv
from datetime import datetime,timedelta
import pandas as pd
import os
import winsound
import time
from dateutil.relativedelta import relativedelta
import requests

currencyPath = 'usd_rate.csv'
SoundRepeat     = 5 #讀完資料後，提示聲要響多少次
SoundInterval   = 5 #多久響一次(秒)

url = 'https://v6.exchangerate-api.com/v6/e36e2ab2049b8500336a29c2/latest/USD'
currencyPath = 'usd_rate.csv'

response = requests.get(url)
data = response.json()
rates = data['conversion_rates']
print(rates.keys())
header = ["currency", "rate"]
with open(currencyPath, 'w') as f:
    # Write all the dictionary keys in a file with commas separated.
    f.write(','.join(header))
    f.write('\n') # Add a new line
    for row in rates.keys():
        # Write the values in a row.
        f.write(row+','+str(rates[row])+'\n')
        
print('saved.')    
for i in range(SoundRepeat):
    winsound.PlaySound("SystemAsterisk", winsound.SND_ASYNC)
    time.sleep(SoundInterval)
