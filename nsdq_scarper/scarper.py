import httpx
from typing import List
from models import DividendRecord


class Scarper:
    BASE_URL = "https://api.nasdaq.com/api/quote"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Content-Type":"application/json",
        "Accept": "application/json",
    }

    async def get_json_from_page(self, client: httpx.AsyncClient, url: str) -> dict:
        """Fetch and return JSON from a page"""
        response = await client.get(url)
        response.raise_for_status()
        return response.json()

    async def fetch_dividends(self, tick: str) -> List[DividendRecord]:
        """Scarpe dividends"""
        url = f"{self.BASE_URL}/{tick}/dividends?assetclass=stocks"
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, read=20.0),
            headers=self.HEADERS,
            follow_redirects=True,
        ) as client:
            dividends = await self.get_json_from_page(client, url)
            records = []
            for dividend in dividends['data']['dividends']['rows']:
                try:
                    record = DividendRecord(
                        ex_dividend_date=dividend.get('exOrEffDate', ''),
                        payment_type=dividend.get('type', ''),
                        amount=str(dividend.get('amount', '')).replace('$',''),
                        declaration_date=dividend.get('declarationDate', '').strip(),
                        record_date=dividend.get('recordDate', '').strip(),
                        payment_date=dividend.get('paymentDate', '').strip(),
                        currency=dividend.get('currency', '').strip(),
                    )
                    records.append(record)
                except Exception as e:
                    print(f"Error processing dividend data: {str(e)}")
            return records
 
