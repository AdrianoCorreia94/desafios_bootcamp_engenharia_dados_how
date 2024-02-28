import pandas as pd
from sqlalchemy import create_engine


engine = create_engine('postgresql+psycopg2://root:root@localhost/nba')

# ler dataset
payroll = pd.read_csv('./db/archive/NBA Payroll(1990-2023).csv')

# transformar dataset em dataframe
df = pd.DataFrame(payroll)

# transformar as colunas string em float
df[['payroll', 'inflationAdjPayroll']] = df[['payroll',
                                             'inflationAdjPayroll']].replace('[\$,]', '', regex=True).astype(float)


# transformar a coluna em ano
df['seasonStartYear'] = pd.to_datetime(df['seasonStartYear'], format='%Y')

# transformar somente para o ano
df['seasonStartYear'] = df['seasonStartYear'].dt.year

# incluir colunas para o database
df = df[['team',
         'seasonStartYear',
         'payroll',
         'inflationAdjPayroll']]

# visualizar o dataset
print(df)

# enviar para o banco de dados
df.to_sql('payroll',  # nome da tabela
          con=engine,   # ferramenta de conexao
          index=True,   # autoincremento
          if_exists='replace')
