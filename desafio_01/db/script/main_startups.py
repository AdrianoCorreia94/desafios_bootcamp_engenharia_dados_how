import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import dml
import os


def criar_engine(user=os.environ.get('user'),
                 pwd=os.environ.get('pwd'),
                 host=os.environ.get('host'),
                 db=os.environ.get('db')):
    return create_engine(f'postgresql+psycopg2://{user}:{pwd}@{host}/{db}')


def enviar_startups(schema_target):
    df = pd.read_json('./db/startups/json_data.json')
    df = df.drop(columns=['id'])

    for col in df.columns:
        df[col] = df[col].astype(str)

    df['date'] = datetime.now().date()

    df.to_sql(
        'startups',
        if_exists='replace',
        con=engine,
        index=True,
        schema=schema_target
    )


engine = criar_engine()

schema_target = "startups"
dml.create_schema(schema_target)
enviar_startups(schema_target)
