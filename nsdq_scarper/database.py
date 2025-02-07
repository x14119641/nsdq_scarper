from typing import List
import asyncpg
import asyncio
import os


class Database:
    def __init__(self, host, user, password, database, port=5432) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.pool = None

    
    async def create_pool(self):
        """Create a connection pool."""
        self.pool = await asyncpg.create_pool(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
            max_size=10,  # Max pool size
            max_queries=50000,  # Max queries per connection before being closed
            min_size=1,  # Minimum pool size
            timeout=60.0  # Timeout for acquiring a connection from the pool
        )
    
    async def create_database(self):
        """Create the database if it does not exist"""
        # Connect to the default 'postgres' database first to create the new one
        connection = await asyncpg.connect(user=self.user, password=self.password, database="postgres", host="127.0.0.1")
        try:
            await connection.execute(f"CREATE DATABASE {self.database};")
            print(f"Database {self.database} created successfully.")
        except asyncpg.exceptions.DuplicateDatabaseError:
            print(f"Database {self.database} already exists.")
        finally:
            await connection.close()
            

    async def show_dbs(self):
        """Shows list of tables in the 'nsdq' schema"""
        data = await self.pool.fetch("SELECT * FROM information_schema.tables WHERE table_schema = 'public'")
        print(data)

            
    async def connect(self):
        """Establish the database connection"""
        self.pool = await asyncpg.create_pool(
            user=self.user, 
            password=self.password, 
            database=self.database, 
            host="127.0.0.1"
        )
        print("Database connected")

    async def close_pool(self):
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()
            print(f"Pool closed.")
            
    async def execute(self, query, *args):
        """Execute a query without returning results."""
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

    
    async def executemany(self, query, *args):
        """Execute a query without returning results."""
        async with self.pool.acquire() as conn:
            # print(f"Executing query: {query}")
            return await conn.executemany(query, *args)
        
        
    async def fetch(self, query, *args):
        """Execute a query and return results."""
        async with self.pool.acquire() as conn:
            records = await conn.fetch(query, *args)
            if records:
                return [dict(record) for record in records]
            return None
 
    async def fetchone(self, query, *args):
        """Execute a query and return one result."""
        async with self.pool.acquire() as conn:
            # print(f"Fetching query: {query}")
            val = await conn.fetchval(query, *args)
            return val if val else None
    
    async def fetchrow(self, query, *args):
        """Execute a query and return one row."""
        async with self.pool.acquire() as conn:
            # print(f"Fetching query: {query}")
            record = await conn.fetchrow(query, *args)
            return dict(record) if record else None
        
    
    async def create_schema(self):
        """Create the database schema."""
        create_schema = self.read_sql('schema')
        await self.execute(create_schema)
        print('Schema Created')

        
    def read_sql(self, file_name):
        file_dir = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(file_dir, f'sql_queries/{file_name}.sql'), 'r') as f:
            data = f.read()
        return data


# Main execution flow
async def main():
    db = Database('127.0.0.1','test', 'test', 'nsqd') 
    await db.create_database()
    await db.create_pool() 
    await db.create_schema()
    await db.show_dbs() 
    await db.close_pool()  



async def set_up_database():
    db = Database('test', 'pwd', 'nsdq') 
    await db.create_database()
    await db.create_pool() 
    await db.show_dbs() 
    await db.close_pool()  
    
    
if __name__ == '__main__':
    asyncio.run(main())  
