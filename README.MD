# ETL exercise: Weather data 

1. Downloads gz files from weather data website
2. Extracts gz files to csv files
3. Concat all csv files
4. Load csv to pandas Dataframe 
3. Map weather station name to ISO country code
4. Sort by date

This script downloads all .gz weather files from a data range that can be set in the variables or from the CLI.

## 1. How to run the script

Execute the following command to get the right package dependencies
```
pip install -r requirements.txt
```
Also make sure you are using Python version: 3.6.5

Before executing the following command make sure you are in the right directory 
```
python main.py
```