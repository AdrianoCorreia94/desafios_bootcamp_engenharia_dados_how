from sqlalchemy import create_engine
import pandas as pd


engine = create_engine('postgresql+psycopg2://root:root@localhost/desafio_01')

sql = '''
select * from nba_dataset.payroll
'''

df = pd.read_sql_query(sql, engine)

print(df)