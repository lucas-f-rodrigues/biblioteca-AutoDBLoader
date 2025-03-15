import mysql.connector
from mysql.connector import Error
import pandas as pd

def get_tables_name(cursor):
    tebles_totais = []
    tables = cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE();") or cursor.fetchall()
    for table in tables:
        tebles_totais.append(table[0])
    return tebles_totais


def get_foreign_keys(cursor):
    colunas_com_forengKey = set([])
    foreign_keys = cursor.execute("""
                SELECT 
                    kcu.table_name AS tabela_origem,
                    kcu.column_name AS coluna_origem,
                    kcu.referenced_table_name AS tabela_referenciada,
                    kcu.referenced_column_name AS coluna_referenciada
                FROM 
                    information_schema.key_column_usage AS kcu
                WHERE 
                    kcu.table_schema = DATABASE() 
                    AND kcu.referenced_table_name IS NOT NULL;
            """) or cursor.fetchall()

    for fk in foreign_keys:
        colunas_com_forengKey.add(fk[0])

    return list(colunas_com_forengKey), foreign_keys


def post_dados(cursor, table_json):
    for table in table_json:
        
        tabela = table["name_table"]
        primaryKey = table["primary_key"]
        unwanted_attributes = table["unwanted_attributes"]
        path_arquivo = table["path_file"]
        sep_csv = table["file_sep"]

        df = pd.read_csv(path_arquivo, sep=sep_csv)
        df = df[sorted(df.columns)]
        colunas = list(df.columns)

        if primaryKey in colunas:
            colunas.remove(primaryKey)
        for nulls in unwanted_attributes:
            if nulls in colunas:
                colunas.remove(nulls)

        query = f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES ({', '.join(['%s' for _ in colunas])})"
        valores = [tuple(row) for row in df.itertuples(index=False, name=None)]

        cursor.executemany(query, valores)

        print(f"{cursor.rowcount} registros inseridos com sucesso!")


def main(json):
    try:
        db = json["db"]
        conn = mysql.connector.connect(
            host=db["host"],
            user=db["user"],
            password=db["password"],
            database=db["database"],
            port=db["port"],
            charset="utf8mb4",
            collation="utf8mb4_general_ci"
        )
        
        if conn.is_connected():
            cursor = conn.cursor()

            tebles_names = get_tables_name(cursor)
            print(tebles_names)
            
            colunas_com_forengKey, foreign_keys = get_foreign_keys(cursor)
            print(colunas_com_forengKey)

            resultado = list(set(tebles_names) - set(colunas_com_forengKey))

            print(resultado)

            df = pd.DataFrame(foreign_keys, columns=["tabela", "chave_estrangeira", "tabela_estrangeira", "id_tabela_estrangeira"])

            print(df)
            post_dados(cursor, json["tables"])

            cursor.close()
        else:
            print("Não foi possível estabelecer a conexão.")
    except Error as e:
        print(f"Erro ao conectar ou executar a consulta: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("Conexão encerrada.")

# Parâmetros de conexão
conn_params = {
    "host": "cloud.fslab.dev",
    "user": "plataforma_matematica",
    "password": "admin",
    "database": "plataforma_matematica",
    "port": 8806
}

main({
  "db": {
    "host": "cloud.fslab.dev",
    "user": "plataforma_matematica",
    "password": "admin",
    "database": "plataforma_matematica",
    "port": 8806
  },
  "tables": [
        {
            "name_table": "usuario",
            "primary_key": "id",
            "unwanted_attributes": ['refresh_token'],
            "path_file": "./usuarios.csv",
            "type_file":"csv",
            "file_sep": ";"
        },
        {
            "name_table": "grupo",
            "primary_key": "id",
            "unwanted_attributes": [],
            "path_file": "./grupos.csv",
            "type_file":"csv",
            "file_sep": ";"
        }
    ]
  })