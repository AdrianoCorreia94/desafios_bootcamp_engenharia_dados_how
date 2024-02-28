import psycopg2


def create_schema(schema):
    conexao = psycopg2.connect(
        host='localhost',
        database="desafio_01",
        port=5432,
        user="root",
        password="root")

    # Criando um cursor
    cursor = conexao.cursor()

    sql = f'''
        CREATE SCHEMA IF NOT EXISTS {schema}
        '''

    # Realizando a consulta na tabela do postgres
    cursor.execute(sql)

    conexao.commit()
    conexao.close()
