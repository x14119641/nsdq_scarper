import asyncio
from scarper import Scarper


async def main():
    scraper = Scarper()
    data = await scraper.fetch_dividends('VALE')
    print(f"Scraped {len(data)} records")


# Run the async main
if __name__ == "__main__":
    asyncio.run(main())