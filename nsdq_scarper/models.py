from pydantic import BaseModel


class DividendRecord(BaseModel):
    ex_dividend_date:str
    payment_type:str
    amount:str
    declaration_date:str
    record_date:str
    payment_date:str
    currency:str
    
    