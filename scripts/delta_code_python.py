import traceback
import psycopg2
import logging
import pandas as pd
from psycopg2 import sql
from pprint import pprint

_author_ = 'vbs'

df = pd.read_csv(f"/country_codes.csv")
country_codes = list(df['country_code'])

append_sql = """
CREATE TABLE available_markets(
track_id VARCHAR(255) PRIMARY KEY references track(track_id),

"""

for code in country_codes:
    append_sql += f"{code}_ BOOLEAN, "

append_sql = append_sql[:-2]
append_sql += ");"

print(append_sql)
sql_create_table(append_sql)
