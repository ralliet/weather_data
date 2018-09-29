import multiprocessing  # parallel download filess
import requests
import gzip  # unzip files
import shutil
import os
import glob  # read all files from specific dir
import pandas as pd


# download weather files using multiprocess
def worker(download_url):
    """thread worker function"""
    r = requests.get(download_url)
    filename_gz = download_url.split('/')[-1]
    filename_csv = filename_gz.replace('.gz', '')

    # store gzip file
    open(r'data\{}'.format(filename_gz), 'wb').write(r.content)

    # open gzip file and convert to csv file
    with gzip.open(r'data\{}'.format(filename_gz), 'rb') as f_in:
        with open('data\{}'.format(filename_csv), 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # remove all gzip files
    os.remove(r'data\{}'.format(filename_gz))


def transform():
    file_output = open("data\output\weather_temp.csv", "w")
    # concat all csv files
    for filename in glob.glob('data\*.csv'):
        for line in open(filename):
            file_output.write(line)
    file_output.close()

    # read weather_temp.csv file(concat of all downloaded csv files)
    df_weather = pd.read_csv('data\output\weather_temp.csv', names =["weather_station_code","date","value_type","value","not_measured_1","not_measured_2","measure_unit","not_measured_3"])

    weather_station_code_to_country = {
        "EZE00100082": "CZ",
        "ITE00100550": "IT"
    }
    # filter rows on weather station code ("EZE00100082")
    df_weather = df_weather[df_weather.weather_station_code.isin(weather_station_code_to_country.keys())]
    # map weather station code to country "EZE00100082" => CZ
    df_weather["country"] = df_weather.weather_station_code.map(weather_station_code_to_country)
    # select all Temperature minimums (TAVG value not available in 19th century of dataset)
    df_weather = df_weather[df_weather.value_type == "TMIN"]
    # sort by date
    df_weather.sort_values(by=['date'])

    df_weather['date'] = pd.to_datetime(df_weather['date'].astype(str), format='%Y-%m')

    # select columns country,date and value
    df_weather = df_weather[['country', 'date','value']]
    # write to csv file
    df_weather.to_csv(r'data\output\weather.csv', sep=',', encoding='utf-8', index=False)

    # remove all gzip files
    os.remove(r'data\output\weather_temp.csv')

if __name__ == '__main__':
    start_year = 1820
    end_year = 1821
    extra_year = 1822
    # Weather data UI
    # print("----------------------------------------------------------------")
    # print("----------------------- WEATHER DATA ---------------------------")
    # print("----------------------------------------------------------------")
    # while True:
    #     try:
    #         start_year = int(input("Give a Start year(1800-2018):\t"))
    #         if start_year < 1800 or start_year > 2018:
    #             print('start year is not in the range of 1800-2018')
    #             continue
    #         end_year = int(input("Give a End year(>start year):\t"))
    #         if (start_year > end_year):
    #             print('start year is smaller then end year')
    #             continue
    #         extra_year = int(input("get data from another year (1800-2018):\t"))
    #         if extra_year < 1800 or extra_year > 2018:
    #             print('extra year is not in the range of 1800-2018')
    #             continue
    #         break
    #     except ValueError:
    #         print("Oops!  That was no valid number.  Try again...")

    # build list of download urls
    url_download_list = [r'http://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/{}.csv.gz'.format(year) for year in
                         range(start_year, end_year)]

    url_download_list.append(r'http://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/{}.csv.gz'.format(extra_year))
    jobs = []

    for download_url in url_download_list:
        p = multiprocessing.Process(target=worker, args=(download_url,))
        jobs.append(p)
        p.start()

        # wait for workers to be finished
        for job in jobs:
            job.join()

    # aggregate functions on the weather data
    transform()
