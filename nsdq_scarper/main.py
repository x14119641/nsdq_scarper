import asyncio
import itertools
import time
import argparse
import polars as pl
from scarper import Scarper
from database import Database





async def insert_tickers(db, scraper, batch_size=10):
    """Inserts tickers data"""
    tickers = pl.scan_csv('nasdaq_screener_1738844749646.csv').select('Symbol').collect()

    tickers = tickers['Symbol'].to_list()
    for chunk in itertools.batched(tickers, batch_size):
        tickers_records = await scraper.fetch_multiple_tickers(chunk)
        query = """INSERT INTO tickers (ticker, companyName, stockType, exchange, assetClass, isNasdaqListed, isNasdaq100, isHeld) 
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8) 
            ON CONFLICT (ticker) DO NOTHING"""
        data = [tuple(record.values()) for record in tickers_records if len(record) >0]
        await db.executemany(query, data)

    print("Tickers Inserted")
    
    
async def insert_old_institutionals(db):
    """Inserts institutional_holdings old  data"""
    df = pl.read_csv('institutional_holdings_old.csv', try_parse_dates=True) #string to datetime

    #  Clean the error 
    df = df.with_columns(
        pl.col('sharesOutstandingTotal').str.replace_all(",", "").cast(pl.Int64),
        pl.col('increasedPositionsHolders').str.replace_all(",", "").cast(pl.Int64),
        pl.col('totalHoldingsValue').str.replace_all(",", "").cast(pl.Int64),
        pl.col('totalPositionsHolders').str.replace_all(",", "").cast(pl.Int64),
        pl.col('decreasedPositionsHolders').str.replace_all(",", "").cast(pl.Int64))
    # Filter only if i have those tickers
    tickers = await db.fetch("SELECT ticker FROM tickers")
    tickers = [item['ticker'] for item in tickers]

    df_filtered = df.filter(pl.col("ticker").is_in(tickers))

    records = [tuple(row)for row in df_filtered.iter_rows()]
    
    query = """INSERT INTO institutional_holdings (ticker, sharesOutstandingPCT, sharesOutstandingTotal, totalHoldingsValue, increasedPositionsHolders, increasedPositionsShares, decreasedPositionsHolders, decreasedPositionsShares, heldPositionsHolders, heldPositionsShares, totalPositionsHolders, totalPositionsShares, newPositionsHolders, newPositionsShares, soldOutPositionsHolders, soldOutPositionsShares, inserted) 
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) 
            """

    await db.executemany(query, records)
    print("Old Data Inserted")
       
async def scrape_dividends(db, scraper, batch_size=10):
    """Fetch dividends for a list of tickers and store them in the database."""
    counter = 0
    tickers = await db.fetch("SELECT ticker FROM tickers;")
    for chunk in itertools.batched(tickers, batch_size):
        dividends = await scraper.fetch_multiple_dividends(chunk)
        print(dividends[0:2])
        clean_records = [(item['ticker'], item['exOrEffDate'], item['paymentType'], item['amount'],
                        item['declarationDate'], item['recordDate'], item['paymentDate'], item['currency'])
                        for sublist in dividends if len(sublist)>0 for item in sublist]
        time.sleep(1)
        await db.executemany(
            """INSERT INTO dividends (ticker, exOrEffDate, paymentType, amount, declarationDate, recordDate, paymentDate, currency) 
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8) 
               ON CONFLICT (ticker, exOrEffDate, paymentType, amount, declarationDate, recordDate, paymentDate, currency) DO NOTHING""", 
            clean_records
        )
        counter += len(clean_records)
    
    print(f"Scraped and saved {counter} dividend records")
    


async def scrape_institutionals(db, scraper,batch_size=10):
    """Fetch metadata for tickers and store it in the database."""
    tickers = await db.fetch("SELECT ticker FROM tickers;")
    
    
    #  asyncpg.exceptions.DataError: invalid input for query argument $3 in element #2 of executemany() sequence: '15,022'
    counter = 0
    for chunk in itertools.batched(tickers, batch_size):
        institutionals_records = await scraper.fetch_multiple_institutionals(chunk)
        clean_records = [tuple(record.values()) for record in institutionals_records if len(record)>0]
        time.sleep(1)
        await db.executemany(
            """INSERT INTO institutional_holdings (ticker, sharesOutstandingPCT, sharesOutstandingTotal, totalHoldingsValue, increasedPositionsHolders, increasedPositionsShares, decreasedPositionsHolders, decreasedPositionsShares, heldPositionsHolders, heldPositionsShares, totalPositionsHolders, totalPositionsShares, newPositionsHolders, newPositionsShares, soldOutPositionsHolders, soldOutPositionsShares) 
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) 
            """, clean_records    
        )
        
        counter += len(clean_records)
    print(f"Scraped and saved metadata for {chunk} tickers")
    
    
async def scrape_metadata(db, scraper, batch_size=10):
    """Fetch metadata for tickers and store it in the database."""
    tickers = await db.fetch("SELECT ticker FROM tickers;")
    counter = 0
    for chunk in itertools.batched(tickers, batch_size):
        metadata_records = await scraper.fetch_multiple_metadata(chunk)
        clean_records = [tuple(record.values()) for record in metadata_records if len(record)>0]
        time.sleep(1)
        await db.executemany(
            """INSERT INTO metadata (ticker, exchange, sector, industry, oneYrTarget, todayHighLow, shareVolume, 
                                    averageVolume, previousClose, fiftTwoWeekHighLow, marketCap, PERatio, 
                                    forwardPE1Yr, earningsPerShare, annualizedDividend, exDividendDate, 
                                    dividendPaymentDate, yield, specialDividendDate, specialDividendAmount, 
                                    specialDividendPaymentDate) 
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21) 
            """, clean_records
        )
        counter += len(clean_records)

    print(f"Scraped and saved metadata for {counter} tickers")


async def main(action:str, batch_size:int=10):
    scraper = Scarper()
    db = Database('127.0.0.1','test', 'test', 'nsdq') 
    
    await db.create_pool() 
    
    if action == "create_schema":
        await db.create_database()
        await db.create_schema()
    elif action == "load_old":
        await insert_old_institutionals(db)
    elif action == "tickers":
        start_counter = await db.fetchone('SELECT COUNT(1) FROM tickers')
        start_counter = start_counter or 0
        await insert_tickers(db, scraper, batch_size)
        end_counter = await db.fetchone('SELECT COUNT(1) FROM tickers')
        print(f'Total Inserted: {end_counter-start_counter}')
    elif action == "institutionals":
        await scrape_institutionals(db, scraper)
    elif action == "dividends":
        await scrape_dividends(db, scraper, batch_size)
    elif action == "metadata":        
        await scrape_metadata(db, scraper)
    elif action == "test_institutional":
        ticker = input("Enter a ticker to test institutional fetch: ").strip().upper()
        print(await scraper.fetch_institutionals(ticker)) 
    elif action == "test_info":
        ticker = input("Enter a ticker to test info fetch: ").strip().upper()
        print(await scraper.fetch_info(ticker)) 
    elif action == "test_dividend":
        ticker = input("Enter a ticker to test dividend fetch: ").strip().upper()
        print(await scraper.fetch_dividends(ticker))
    elif action == "test_metadata":
        ticker = input("Enter a ticker to test metadata fetch: ").strip().upper()
        print(await scraper.fetch_metadata(ticker))
    else:
        print("Invalid action. ",
              "Use 'create_schema', 'tickers', 'dividends', 'metadata', 'test_dividend', or 'test_metadata'.")

    
    # print(f"Scraped and saved {counter} records")
    # # print(scraper.INVALID_TICKERS)
    # if scraper.INVALID_TICKERS:
    #         with open("invalid_tickers.csv", "w") as csvfile:
    #             # Write the header
    #             csvfile.write("Ticker\n")
    #             # Write each ticker on a new line
    #             for ticker in scraper.INVALID_TICKERS:
    #                 csvfile.write(f"{ticker}\n")
    #         print("Invalid tickers written to invalid_tickers.csv")
    # print('F I N I S H E D')


# Run the async main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Scarper with different actions")
    parser.add_argument("action", type=str, help="Chose 'dividends, 'metadata, 'bla'")
    parser.add_argument("--batch_size", type=int, default=10, help="Number of tickers per batch")
    args = parser.parse_args()
    asyncio.run(main(args.action, args.batch_size))