import psycopg2
import csv

# Dados de conexão com o banco PostgreSQL
dbname='postgres'
user='postgres'
password='12345'
host='localhost'
port='5432'

# Arquivo CSV contendo os dados de alunos
file = '1104_Alunos.csv'
path = f'data\{file}'


def create_table_psql(dbname: str = dbname,
                      user: str = user,
                      password: str = password,
                      host: str = host,
                      port: str = port):
    

    # conexão com o banco de dados PostgreSQL
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
    except Exception as e:
        raise print(f'Not able to make connection with PostgreSQL. Error {e}')

    # criação da tabela no banco de dados (se não existir)
    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS tb_alunos (
                nome VARCHAR(255),
                idade VARCHAR(50),
                idade_padronizada VARCHAR(50),
                estado VARCHAR(50),
                uf VARCHAR(5),
                graduacao VARCHAR(255),
                hobby VARCHAR(255),
                profissao_atual VARCHAR(255),
                profissao_pretendida VARCHAR(255),
                min_salario_pret FLOAT,
                max_salario_pret FLOAT
            )
        ''')

    conn.commit()
    conn.close()


def load_to_postgres(dbname: str = dbname,
                      user: str = user,
                      password: str = password,
                      host: str = host,
                      port: str = port,
                      path: str = path):

    # Conectar ao PostgreSQL
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
    except Exception as e:
        raise print(f'Not able to make connection with PostgreSQL. Error {e}')
    
    # Leitura dos dados do arquivo CSV e inserção no banco de dados
    try:
        with open(path, "r", encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file)
            next(reader)  # Pula o cabeçalho do CSV
            with conn.cursor() as cur:
                for row in reader:
                    cur.execute('''
                        INSERT INTO tb_alunos (
                                    nome,
                                    idade,
                                    idade_padronizada,
                                    estado,
                                    uf,
                                    graduacao,
                                    hobby,
                                    profissao_atual,
                                    profissao_pretendida,
                                    min_salario_pret,
                                    max_salario_pret)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (row[0], row[1], row[2], row[3], row[4],
                        row[5], row[6], row[7], row[8], row[9], row[10]))
            conn.commit()
    except Exception as e:
        raise print(f'Not able to insert data in table. Error {e}')
    conn.close()