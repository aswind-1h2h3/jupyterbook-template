import os
import requests
import datetime
import xarray as xr
import cfgrib
import earthkit.data as ekd

# The aim of the script is to demonstrate how to download the open data from AIFS models using requests.
# One file is read and transformed into a xarray dataset.
# An example of downloading open data from IFS models is not yet developed.

DATADIR = os.path.abspath(os.path.dirname(__file__))

today = datetime.date.today().strftime('%Y%m%d') # a user can choose current date or data up to four days before today 
timez = "00z/" # user can choose an element from the list ["00z/", "06z/", "12z/", "18z/"]
model = "aifs-single/" # user can choose between "aifs-single/" and "ifs/"
resol = "0p25/"
stream_ = "oper" # a user can choose an element from the list of options [ "oper", "enfo", "scda", "scwv", "waef", "wave"]
type_ = "fc" # user can choose an element from the list ["fc", "ef"]
step = 6 # user can choose between 3 and 6 according to the selected product (ensemble/HRES or AIFS)

"""Ensemble products and HRES products:
    For times 00z & 12z: 0 to 144 by 3, 150 to 360 by 6. (IFS: enfo/oper/waef/wave)
    For times 06z & 18z: 0 to 144 by 3. (IFS: enfo/scada/scwv/waef)

   Determenistic AIFS (oper):
    For times 00z & 06z & 12z & 18z: 0 to 360 by 6.
"""

list_of_files = [today + timez[:-2] + "0000-" + str(hour) + "h-" + stream_ + "-" + type_ + ".grib2" for hour in range(0, 366, step)]

with requests.Session() as s:
    for filename in list_of_files:
        try:
            start = datetime.datetime.now()
            response = requests.get("https://data.ecmwf.int/ecpds/home/opendata/" + today + "/" + timez +
                                    model + resol + stream_ + "/" + filename, stream=True)
            if response.status_code == 200:
                with open(filename, mode="wb") as file:
                    for chunk in response.iter_content(chunk_size=10 * 1024):
                        file.write(chunk)
                end = datetime.datetime.now()
                diff = end - start
                print('The ' + str(filename) + ' file downloaded in ' + str(diff.seconds) + ' seconds.')
        except:
            print("There is no file {0} to download.".format(filename))
#######################################################################
# Examples:
"""Multiple values for a unique key:
filter_by_keys={'typeOfLevel': 'soilLayer'}
filter_by_keys={'typeOfLevel': 'heightAboveGround'}
filter_by_keys={'typeOfLevel': 'isobaricInhPa'}
filter_by_keys={'typeOfLevel': 'surface'}
filter_by_keys={'typeOfLevel': 'lowCloudLayer'}
filter_by_keys={'typeOfLevel': 'mediumCloudLayer'}
filter_by_keys={'typeOfLevel': 'highCloudLayer'}
filter_by_keys={'typeOfLevel': 'entireAtmosphere'}
filter_by_keys={'typeOfLevel': 'meanSea'}
"""
filename = list_of_files[0]
ds1 = xr.open_dataset(str(DATADIR) + '/' + filename, engine='cfgrib',
                     backend_kwargs={'filter_by_keys': {'typeOfLevel': 'surface'}})

ds2 = cfgrib.open_datasets(str(DATADIR) + '/' + filename)

ekd.download_example_file(str(DATADIR) + '/' + filename)
ds3 = ekd.from_source("file", str(DATADIR) + '/' + filename)
print(ds3.head())
