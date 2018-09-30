# ETL exercise: Weather data 

This script downloads .gz weather files from a specific year range that can be set in the variables or from the CLI.

1. Downloads gz files from weather data website
2. Extracts gz files to csv files
3. Load each downloaded csv filter as dataframe to filter it
    - Map weather station name to ISO country code
    - Convert date
4. Store filtered csv file
5. Merge all filtered csv files to data\output\weather.csv


## 1. How to run the script

Execute the following command to get the right package dependencies
```
pipenv install
```
Also make sure you are using Python version: 3.6.5

Before executing the following command make sure you are in the right directory 
```
python main.py
```