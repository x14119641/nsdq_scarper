import asyncpg
import asyncio
from typing import List
from models import DividendRecord  # Ensure this is correctly defined

class Database:
    def __init__(self, username: str, password: str, dbname: str) -> None:
        self.username = username
        self.password = password
        self.dbname = dbname
        self.pool = None

    
    async def create_database(self):
        """Create the database if it does not exist"""
        # Connect to the default 'postgres' database first to create the new one
        connection = await asyncpg.connect(user=self.username, password=self.password, database="postgres", host="127.0.0.1")
        try:
            await connection.execute(f"CREATE DATABASE {self.dbname};")
            print(f"Database {self.dbname} created successfully.")
        except asyncpg.exceptions.DuplicateDatabaseError:
            print(f"Database {self.dbname} already exists.")
        finally:
            await connection.close()
            
    async def create_schema(self):
        create_tickers_sql = """
            CREATE TABLE IF NOT EXISTS tickers (
                tick TEXT PRIMARY KEY, 
                name TEXT
            )
        """
        create_metadata_sql = """
            CREATE TABLE IF NOT EXISTS metadata (
                id SERIAL PRIMARY KEY,
                tick TEXT NOT NULL, 
                name TEXT,
                last_sale DECIMAL(12,2),
                net_change DECIMAL(12,2),
                change_perc DECIMAL(12,2),
                market_cap INTEGER,
                country TEXT,
                ipo_year INTEGER,
                volume INTEGER,
                sector TEXT,
                industry TEXT,
                inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (tick)
                REFERENCES tickers (tick)
            )
        """
        create_dividends_sql = """
            CREATE TABLE IF NOT EXISTS nsdq.dividends (
                id SERIAL PRIMARY KEY,
                tick TEXT NOT NULL, 
                ex_dividend_date DATE,
                payment_type TEXT,
                amount DECIMAL(12,2),
                declaration_date DATE,
                record_date DATE,
                payment_date DATE,
                currency TEXT,
                inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (tick, ex_dividend_date),
                FOREIGN KEY (tick)
                REFERENCES tickers (tick)
            )
        """
        async with self.pool.acquire() as connection:
            try:
                await connection.execute(create_tickers_sql)
                await connection.execute(create_metadata_sql)
                await connection.execute(create_dividends_sql)
                print("Schema and tables created successfully.")
            except Exception as e:
                print(f"Error creating schema: {str(e)}")

    async def show_dbs(self):
        """Shows list of tables in the 'nsdq' schema"""
        async with self.pool.acquire() as connection:
            try:
                data = await connection.fetch("SELECT * FROM information_schema.tables WHERE table_schema = 'public'")
                print(data)
            except Exception as e:
                print(f"Error showing DBs: {str(e)}")
            
    async def connect(self):
        """Establish the database connection"""
        self.pool = await asyncpg.create_pool(
            user=self.username, 
            password=self.password, 
            database=self.dbname, 
            host="127.0.0.1"
        )
        print("Database connected")

    async def close(self):
        """Close the database connection pool"""
        if self.pool:
            await self.pool.close()
            print("Database pool closed")

    async def insert_dividends(self, dividends: List[DividendRecord]):
        """Inserts dividend records into the database"""
        async with self.pool.acquire() as connection:
            for dividend in dividends:
                try:
                    await connection.execute(
                        """
                        INSERT INTO dividends (
                            tick, ex_dividend_date, payment_type, amount, declaration_date,
                            record_date, payment_date, currency
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    """,
                        dividend.tick,
                        dividend.payment_type,
                        dividend.ex_dividend_date.strftime("%Y-%m-%d"),
                        dividend.amount,
                        dividend.declaration_date.strftime("%Y-%m-%d"),
                        dividend.record_date.strftime("%Y-%m-%d"),
                        dividend.payment_date.strftime("%Y-%m-%d") if dividend.payment_date is not None else None,
                        dividend.currency
                    )
                    print(f"Inserted dividend: {dividend}")
                except Exception as e:
                    print(f"Error inserting dividend: {str(e)}")

# Main execution flow
async def main():
    db = Database('test', 'pwd', 'nsqr') 
    await db.create_database()
    await db.connect() 
    await db.show_dbs() 
    await db.close()  

if __name__ == '__main__':
    asyncio.run(main())  
