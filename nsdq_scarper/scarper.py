import httpx
import asyncio
from typing import List
from models import DividendRecord
from datetime import datetime
from decimal import Decimal

class Scarper:
    BASE_URL = "https://api.nasdaq.com/api/"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Content-Type":"application/json",
        "Accept": "application/json",
    }
    INVALID_TICKERS = []

    async def get_json_from_page(self, client: httpx.AsyncClient, url: str) -> dict:
        """Fetch and return JSON from a page"""
        try:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f'Error in get_json_from_page {str(e)}')
            return []

    def clean_str(self, val:str):
        """Cleans string if empty

        Args:
            val (str): string value

        Returns:
            None: If no val or 'N/A'
            val: If val
        """
        if not val or val == 'N/A':
            return None
        return val
    
    
    def clean_date(self, date_val:str):
        """Clean date string

        Args:
            date_val (str): _description_

        Returns:
            None: If no date or 'N/A'
            datetime: If correct date string format 
        """
        if not date_val or date_val == 'N/A':
            return None
        try:
            if ',' in date_val:
                return datetime.strptime(date_val, '%b %d, %Y')
            return datetime.strptime(date_val, '%m/%d/%Y')
        except ValueError:
            print(f"Warning: Unrecognized date format '{date_val}'")
            return None
    
    def clean_number_str(self, val:str):
        """Clean number string, removes special charactrers like $, % and ','

        Args:
            val (str): Number representation as string

        Returns:
            None: If no val or 'N/A'
            Decimal: If ',' in val
            int: If not ',' in val
        """
        if not val or val == 'N/A':
            return None
        val = val.replace('%', '').replace('$', '').replace(',', '')
        if '.' in val:
            return Decimal(val)
        return int(val)
    
    
    
    async def fetch_dividends(self, ticker: str):
        """Scarpe dividends"""
        url = f"{self.BASE_URL}/quote/{ticker}/dividends?assetclass=stocks"
        print(url)
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, read=20.0),
            headers=self.HEADERS,
            follow_redirects=True,
        ) as client:
            dividends = await self.get_json_from_page(client, url)
            records = []
 
            if not dividends:
                return []
            
            if not dividends.get('data') or not dividends['data'].get('dividends') or not dividends['data']['dividends'].get('rows'):
                print(f"No dividend data found for {ticker}")
                self.INVALID_TICKERS.append(ticker)
                return []
            
            for dividend in dividends['data']['dividends']['rows']:
                try:
                    records.append({
                        'ticker':ticker,
                        'exOrEffDate':self.clean_date(dividend.get('exOrEffDate', '')),
                        'paymentType':self.clean_str(dividend.get('type', '')),
                        'amount':self.clean_number_str(dividend.get('amount', '')),
                        'declarationDate':self.clean_date(dividend.get('declarationDate', '')),
                        'recordDate':self.clean_date(dividend.get('recordDate', '')),
                        'paymentDate':self.clean_date(dividend.get('paymentDate', '')),
                        'currency':self.clean_str(dividend.get('currency', '')),
                    })
                except Exception as e:
                    print(f"Error processing dividend data: {str(e)}")
                    self.INVALID_TICKERS.append(ticker)
                    continue
            return records
    
    async def fetch_metadata(self,ticker: str):
        url = f"{self.BASE_URL}/quote/{ticker}/summary?assetclass=stocks"
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, read=20.0),
            headers=self.HEADERS,
            follow_redirects=True,
        ) as client:
            metadata = await self.get_json_from_page(client, url)
            if not metadata:
                return []
            if not metadata.get('data'):
                print(f'No metadata por ticker {ticker}')
                return []
            summary_data = metadata.get('data', {}).get('summaryData', {})
            if not summary_data:
                print(f'No summary data por ticker {ticker}')
                return []
            record = {
                'ticker':ticker,
                'exchange':self.clean_str(summary_data.get('Exchange', {}).get('value', '')),
                'sector':self.clean_str(summary_data.get('Sector', {}).get('value', '')),
                'industry':self.clean_str(summary_data.get('Industry', {}).get('value', '')),
                'oneYrTarget':self.clean_number_str(summary_data.get('OneYrTarget', {}).get('value', '')),
                'todayHighLow':self.clean_str(summary_data.get('TodayHighLow', {}).get('value', '')),
                'shareVolume':self.clean_number_str(summary_data.get('ShareVolume', {}).get('value', '')),
                'averageVolume':self.clean_number_str(summary_data.get('AverageVolume', {}).get('value', '')),
                'previousClose':self.clean_number_str(summary_data.get('PreviousClose', {}).get('value', '')),
                'fiftTwoWeekHighLow':self.clean_str(summary_data.get('FiftTwoWeekHighLow', {}).get('value', '')),
                'marketCap':self.clean_number_str(summary_data.get('MarketCap', {}).get('value', '')),
                'PERatio':self.clean_number_str(str(summary_data.get('PERatio', {}).get('value', ''))),
                'forwardPE1Yr':self.clean_number_str(summary_data.get('ForwardPE1Yr', {}).get('value', '')),
                'earningsPerShare':self.clean_number_str(summary_data.get('AnnualizedDividend', {}).get('value', '')),
                'annualizedDividend':self.clean_number_str(summary_data.get('AnnualizedDividend', {}).get('value', '')),
                'exDividendDate':self.clean_date(summary_data.get('ExDividendDate', {}).get('value', '')),
                'dividendPaymentDate':self.clean_date(summary_data.get('DividendPaymentDate', {}).get('value', '')),
                'yield':self.clean_number_str(summary_data.get('Yield', {}).get('value', '')),
                'specialDividendDate':self.clean_date(summary_data.get('SpecialDividendDate', {}).get('value', '')),
                'specialDividendAmount':self.clean_number_str(summary_data.get('SpecialDividendAmount', {}).get('value', '')),
                'specialDividendPaymentDate':self.clean_date(summary_data.get('SpecialDividendPaymentDate', {}).get('value', '')),
            }
            return record
    
    async def fetch_info(self, ticker:str):
        url = f"{self.BASE_URL}/quote/{ticker}/info?assetclass=stocks"
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, read=20.0),
            headers=self.HEADERS,
            follow_redirects=True,
        ) as client:
            info = await self.get_json_from_page(client, url)
            if not info:
                return []
            if not info.get('data'):
                print(f'No info por ticker {ticker}')
                return []
            info = info.get('data')
            record = {
                'ticker':ticker,
                'companyName': self.clean_str(info.get('companyName', '')),
                'stockType':self.clean_str(info.get('stockType', '')),
                'exchange':self.clean_str(info.get('exchange', '')),
                'assetClass': info.get('assetClass', ''),
                'isNasdaqListed': info.get('isNasdaqListed', ''),
                'isNasdaq100': info.get('isNasdaq100', ''),
                'isHeld': info.get('isHeld', ''),
            }
            return record

    async def fetch_institutionals(self,ticker: str):
        url = f"{self.BASE_URL}/company/{ticker}/institutional-holdings?limit=10&type=TOTAL&sortColumn=marketValue"
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, read=20.0),
            headers=self.HEADERS,
            follow_redirects=True,
        ) as client:
            json_data = await self.get_json_from_page(client, url)
            if not json_data:
                return []
            if not json_data.get('data'):
                print(f'No institutional data for ticker {ticker}')
                return []
            ownershipSummary = json_data.get('data', {}).get('ownershipSummary',{})
            activePositions = json_data.get('data', {}).get('activePositions',{}).get('rows', {})
            newSoldOutPositions = json_data.get('data', {}).get('newSoldOutPositions',{}).get('rows', {})

            final_list = activePositions + newSoldOutPositions

            for item in final_list:
                if item.get('positions','') == 'Increased Positions':
                    IncreasedPositionsHolders=self.clean_number_str(item.get('holders', ''))
                    IncreasedPositionsShares=self.clean_number_str(item.get('shares', ''))
                elif item.get('positions') == 'Decreased Positions':
                    DecreasedPositionsHolders=self.clean_number_str(item.get('holders', ''))
                    DecreasedPositionsShares=self.clean_number_str(item.get('shares', ''))
                elif item.get('positions') == 'Held Positions':
                    HeldPositionsHolders=self.clean_number_str(item.get('holders', ''))
                    HeldPositionsShares=self.clean_number_str(item.get('shares', ''))
                elif item.get('positions') == 'Total Institutional Shares':
                    TotalPositionsHolders=self.clean_number_str(item.get('holders', ''))
                    TotalPositionsShares=self.clean_number_str(item.get('shares', ''))
                elif item.get('positions') == 'New Positions':
                    NewPositionsHolders=self.clean_number_str(item.get('holders', ''))
                    NewPositionsShares=self.clean_number_str(item.get('shares', ''))
                elif item.get('positions') == 'Sold Out Positions':
                    SoldOutPositionsHolders=self.clean_number_str(item.get('holders', ''))
                    SoldOutPositionsShares=self.clean_number_str(item.get('shares', ''))
                else:
                    print('Some error getting instutionanals, poisitions not found')
                    
                
            record = {
                'ticker':ticker,
                'SharesOutstandingPCT':self.clean_number_str(ownershipSummary.get('SharesOutstandingPCT', {}).get('value', '')),
                'ShareoutstandingTotal':self.clean_number_str(ownershipSummary.get('ShareoutstandingTotal', {}).get('value', '')),
                'TotalHoldingsValue':self.clean_number_str(ownershipSummary.get('TotalHoldingsValue', {}).get('value', '')),
                'IncreasedPositionsHolders':IncreasedPositionsHolders,
                'IncreasedPositionsShares':IncreasedPositionsShares,
                'DecreasedPositionsHolders':DecreasedPositionsHolders,
                'DecreasedPositionsShares':DecreasedPositionsShares,
                'HeldPositionsHolders':HeldPositionsHolders,
                'HeldPositionsShares':HeldPositionsShares,
                'TotalPositionsHolders':TotalPositionsHolders,
                'TotalPositionsShares':TotalPositionsShares,
                'NewPositionsHolders':NewPositionsHolders,
                'NewPositionsShares':NewPositionsShares,
                'SoldOutPositionsHolders':SoldOutPositionsHolders,
                'SoldOutPositionsShares':SoldOutPositionsShares,
                }
            return record
        
        
    
    async def fetch_multiple_tickers(self, tickers: list):
        tasks = [self.fetch_info(ticker) for ticker in tickers]
        results = await asyncio.gather(*tasks)
        return results
      
    async def fetch_multiple_dividends(self, tickers: list):
        tasks = [self.fetch_dividends(ticker["ticker"]) for ticker in tickers]
        results = await asyncio.gather(*tasks)
        return results

    async def fetch_multiple_metadata(self, tickers: list):
        tasks = [self.fetch_metadata(ticker["ticker"]) for ticker in tickers]
        results = await asyncio.gather(*tasks)
        return results
    
    async def fetch_multiple_institutionals(self, tickers: list):
        tasks = [self.fetch_institutionals(ticker["ticker"]) for ticker in tickers]
        results = await asyncio.gather(*tasks)
        return results
    
    # def __del__(self):
    #     """Write invalid tickers to a CSV file without using external libraries."""
    #     if self.INVALID_TICKERS:
    #         with open("invalid_tickers.csv", "w") as csvfile:
    #             # Write the header
    #             csvfile.write("Ticker\n")
    #             # Write each ticker on a new line
    #             for ticker in self.INVALID_TICKERS:
    #                 csvfile.write(f"{ticker}\n")
    #         print("Invalid tickers written to invalid_tickers.csv")
