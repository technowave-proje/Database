# sqlalchemy ile db ye yazma

import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd

load_dotenv()  # .env dosyasını oku

DATABASE_URL = os.getenv("DATABASE_URL")

def load_data(df: pd.DataFrame, table_name="measurements"):
    """
    DataFrame i sqlalchemy engine kullanarak mysqle yazar
    """
    engine = create_engine(DATABASE_URL)

    # to_sql ile yükle
    df.to_sql(
        table_name,
        con=engine,
        if_exists="append",
        index=False
    )

    print(f"{len(df)} satır {table_name} tablosuna yüklendi.")