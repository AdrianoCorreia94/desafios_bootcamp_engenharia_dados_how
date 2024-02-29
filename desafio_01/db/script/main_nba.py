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


def enviar_payroll(schema_target):
    print('INICIANDO PAYROLL')
    payroll = pd.read_csv('./db/archive/NBA Payroll(1990-2023).csv')

    # transformar dataset em dataframe
    df_payroll = pd.DataFrame(payroll)

    # transformar as colunas string em float
    df_payroll[['payroll', 'inflationAdjPayroll']] = df_payroll[['payroll',
                                                                'inflationAdjPayroll']].replace('[\$,]', '', regex=True).astype(float)

    # transformar a coluna em ano
    df_payroll['seasonStartYear'] = pd.to_datetime(
        df_payroll['seasonStartYear'])

    # transformar somente para o ano
    df_payroll['seasonStartYear'] = df_payroll['seasonStartYear'].dt.year

    # data do registro
    df_payroll['date'] = datetime.now()

    # incluir colunas para o database
    df_payroll = df_payroll[['team',
                            'seasonStartYear',
                             'payroll',
                             'inflationAdjPayroll',
                             'date']]

    # visualizar o dataset
    # print(df_payroll)

    # enviar para o banco de dados
    df_payroll.to_sql('payroll',  # nome da tabela
                      con=engine,   # ferramenta de conexao
                      index=True,   # autoincremento
                      if_exists='replace',
                      schema=schema_target)

    print('PAYROLL FINALIZADO')


def enviar_salaries(schema_target):
    print('INICIANDO SALARIES')

    salaries = pd.read_csv('./db/archive/NBA Salaries(1990-2023).csv')
    df_salaries = pd.DataFrame(salaries)

    # print(df_salaries.head(7))

    # excluindo primeira coluna
    df_salaries = df_salaries.iloc[:, 1:]

    # transformar as 2 ultimas em float
    df_salaries.iloc[:, -2:] = df_salaries.iloc[:, -
                                                2:].replace('[\$,]', '', regex=True).astype(float)

    # transformar coluna seasonStartYear em int
    df_salaries['seasonStartYear'] = pd.to_datetime(
        df_salaries['seasonStartYear']).dt.year

    # data do registro
    df_salaries['date'] = datetime.now()

    # print(df_salaries)

    df_salaries.to_sql(
        'salaries',  # nome da tabela
        con=engine,   # ferramenta de conexao
        index=True,   # autoincremento
        if_exists='replace',
        schema=schema_target)

    print('SALARIES FINALIZADO')


def enviar_boxScore(schema_target):
    print('INICIANDO BOXSCORE')

    box_score = pd.read_csv(
        './db/archive/NBA Player Box Score Stats(1950 - 2022).csv')
    df_boxScore = pd.DataFrame(box_score)
    print(df_boxScore)

    # transformando os dados para os formatos coerentes
    # df_boxScore[['Season','Game_ID','FGM','','']]

    # colunas float
    floats = ['FGA', 'OREB', 'DREB', 'REB', 'AST',
              'STL', 'BLK', 'TOV', 'PF', 'PLUS_MINUS']

    ints = ['Game_ID', 'MIN', 'FGM', 'Season', 'VIDEO_AVAILABLE']

    # print(df_boxScore.iloc[:, 7:])
    for col in floats:
        df_boxScore[col] = df_boxScore[col].astype(float)

    for col in ints:
        df_boxScore[col] = df_boxScore[col].astype(int)

    df_boxScore['GAME_DATE'] = pd.to_datetime(
        df_boxScore['GAME_DATE'], format='%b %d, %Y')

    # print(df_boxScore.iloc[:, 7:])
    # print(df_boxScore.iloc[:, :8])

    df_boxScore = df_boxScore.rename(columns={'Unnamed: 0': 'x'})
    print(df_boxScore)

    df_boxScore['data_register'] = datetime.now()

    chunksize = 500

    df_boxScore.to_sql(
        'box_Score',  # nome da tabela
        con=engine,   # ferramenta de conexao
        index=True,   # autoincremento
        if_exists='replace',
        schema=schema_target,
        chunksize=chunksize
    )

    print('BOXSCORE FINALIZADO')


def enviar_player_stats(schema_target):
    print('INICIANDO PLAYER STATS')
    # carregar dataset como dataframe
    df_player_stats = pd.DataFrame(pd.read_csv(
        './db/archive/NBA Player Stats(1950 - 2022).csv'))

    # ver colunas

    df_player_stats = df_player_stats.drop(columns=['Unnamed: 0.1',
                                                    'Unnamed: 0'])

    ints = [
        'Season',
        'Age',
        'G',
        'GS',
        'MP',
        'FG',
        'FGA',
        '3P',
        '3PA',
        '2P',
        '2PA',
        'FT',
        'FTA',
        'ORB',
        'DRB',
        'TRB',
        'AST',
        'STL',
        'BLK',
        'TOV',
        'PF',
        'PTS',
    ]

    floats = ['FG%', 'eFG%', '3P%', '2P%', 'FT%']

    # tranformar em str, trocar nan por 0, transformar em int e float

    for col in df_player_stats.columns:
        df_player_stats[col] = df_player_stats[col].astype(str)
        if col in ints:
            df_player_stats[col] = df_player_stats[col].replace(
                'nan', '0').astype(float).astype(int)
        elif col in floats:
            df_player_stats[col] = df_player_stats[col].astype(float)

    # data do registro
    df_player_stats['date'] = datetime.now()

    print(df_player_stats.head(10))

    chunksize = 500

    df_player_stats.to_sql(
        'player_stats',  # nome da tabela
        con=engine,   # ferramenta de conexao
        index=True,   # autoincremento
        if_exists='replace',
        schema=schema_target,
        chunksize=chunksize
    )

    print('PLAYER STATS FINALIZADO ')


# criar conexao com db
engine = criar_engine()

# criar schema
schema = "gislaine"
dml.create_schema(schema)

# popular o schema
enviar_payroll(schema)
enviar_salaries(schema)
enviar_boxScore(schema)
enviar_player_stats(schema)
