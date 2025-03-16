import mysql.connector
from mysql.connector import Error
import pandas as pd



## variáveis globais
df_forengKey = None


def get_tables_name(cursor):
    tebles_totais = []
    tables = cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE();") or cursor.fetchall()
    for table in tables:
        tebles_totais.append(table[0])
    return tebles_totais


def get_foreign_keys(cursor):
    global df_forengKey
    
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
    
    df_forengKey = pd.DataFrame(foreign_keys, columns=["tabela", "chave_estrangeira", "tabela_estrangeira", "id_tabela_estrangeira"])

    return list(colunas_com_forengKey)


def open_file_in_df(table_json):
    type_file = table_json["type_file"]
    df = None
    
    if(type_file.upper() == "CSV"):
        df = pd.read_csv(table_json["path_file"], sep=table_json["file_sep"])
        return df[sorted(df.columns)]
    elif(type_file.upper() == "PARQUET"):
        df = pd.read_parquet(table_json["path_file"])
        return df[sorted(df.columns)]
    elif(type_file.upper() == "JSON"):
        df = pd.read_json(table_json["path_file"])
        return df[sorted(df.columns)]
    else:
        raise TypeError(f'Tipo do arquivo "{type_file}" não é valido.') 


def post_dados(cursor, tables_json, conn):
    for table in tables_json:
        try:
            tabela = table["name_table"]
            primaryKey = table["primary_key"]
            unwanted_attributes = table["unwanted_attributes"]

            df = open_file_in_df(table)
            colunas = list(df.columns)

            if primaryKey in colunas:
                colunas.remove(primaryKey)
            for nulls in unwanted_attributes:
                if nulls in colunas:
                    colunas.remove(nulls)

            query = f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES ({', '.join(['%s' for _ in colunas])})"
            valores = [tuple(row) for row in df.itertuples(index=False, name=None)]
            
            if not valores:
                print(f"Nenhum dado para inserir na tabela {tabela}.")
                continue

            cursor.executemany(query, valores)
            conn.commit()

            if cursor.rowcount > 0:
                print(f"{cursor.rowcount} registros inseridos com sucesso na tabela {tabela}!")
            else:
                print(f"Nenhum registro foi inserido na tabela {tabela}. Verifique os dados.")

        except Exception as e:
            print(f"Erro ao inserir dados na tabela {tabela}: {e}")



def db_conect(db_json):
    try:
        db = db_json["db"]
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
            return conn.cursor(), conn
        else:
            print("Não foi possível estabelecer a conexão com o DB.")
    except Error as e:
        print(f"Erro ao se conectar com o banco de dados: {e}")


def main(json):
    try:
        cursor, conn = db_conect(json)

        tebles_names = get_tables_name(cursor)
        print(tebles_names)
        
        colunas_com_forengKey = get_foreign_keys(cursor)
        print(colunas_com_forengKey)

        resultado = list(set(tebles_names) - set(colunas_com_forengKey))

        print(resultado)

        

        print(df_forengKey)
        post_dados(cursor, json["tables"], conn)

        cursor.close()

    except Error as e:
        print(f"Erro ao conectar ou executar a consulta: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("Conexão encerrada.")

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