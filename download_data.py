# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 12:31:00 2025

@author: Logan
"""
    
import pandas as pd
import numpy as np
import requests
import time
from bs4 import BeautifulSoup
import random
from io import BytesIO
import gzip

years = range(2010, 2025)
storm_data = []  # list to store DataFrames

# Setting up random delays.
delays = np.random.normal(3, 1, 20)
delays = [n for n in delays if n > 0]

# The file name includes the date of most recent update. This bit of code will find the file names by year.
url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
r = requests.get(url)
soup = BeautifulSoup(r.text, 'html.parser')
links = [link['href'] for link in soup.find_all('a', text=lambda x: x and 'StormEvents_details' in x)]

for year in years:
    url = f"{url}{[link for link in links if str(year) in link][0]}"
    print(f"Downloading data for {year}...")
    
    r = requests.get(url)
    
    with gzip.open(BytesIO(r.content), mode='rt') as f:
        df = pd.read_csv(f)
        df['year'] = year
        storm_data.append(df)

    time.sleep(random.choice(delays))


all_storms_df = pd.concat(storm_data, ignore_index=True)

all_storms_df.to_csv('Storms_data.csv')