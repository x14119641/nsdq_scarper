# Scarper to get data from nsdq page
Gets data from https://www.nasdaq.com/ and stores the data in postgres.

### Instructions
Install libraries
```
poetry install or pip isntall -r requirements.txt
``` 
## Set up database
```
python main.py create_schema
# Load tickers first
python main.py tickers 
# python main.py load_old
```
## Start actions
```
# Gets 'institutional data'
python main.py institutionals

# Gets 'tickers metadata'
python main.py metadata

# Gets 'tickers dividends'
python main.py dividends
```

