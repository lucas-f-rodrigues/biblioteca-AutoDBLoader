import mysql.connector
from mysql.connector import Error
import pandas as pd



## variáveis globais
df_forengKey = None
tables_finished = []
tables_finished_config = {}
total_tables = 0


def get_tables_name(cursor):
    tables_totais = []
    tables = cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE();") or cursor.fetchall()
    for table in tables:
        tables_totais.append(table[0])
    return tables_totais


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


def verification_tables_finished(table_name):
    df = df_forengKey[df_forengKey["tabela"] == table_name]
    list_tables_foreng = df["tabela_estrangeira"].unique().tolist()
    return set(list_tables_foreng).issubset(set(tables_finished))


## continuar minha função que busca os dados no banco de dados e alterar os ids antigos pelos novos,
## esta com erro ao tentar dropar a colunas forengkey
def get_new_id(df, table_name, cursor):
    df_filter = df_forengKey[df_forengKey["tabela"] == table_name]
    for row in df_filter.itertuples(index=False):
        print(f"tabela:{row.tabela}, chave_estrangeira: {row.chave_estrangeira}, tabela_estrangeira: {row.tabela_estrangeira}, id_tabela_estrangeira:{row.id_tabela_estrangeira}")
        ids_old = cursor.execute(f"""
                SELECT 
                    {row.id_tabela_estrangeira}, id_old_insert
                FROM 
                    {row.tabela_estrangeira}
                WHERE 
                    id_old_insert IS NOT NULL
            """) or cursor.fetchall()
        df_ids_old = pd.DataFrame(ids_old, columns=["primary_id_query", "id_old_insert"])
        print(f"tabela:{row.tabela}")
        print(df)
        print(f"tabela_estrangeira:{row.tabela_estrangeira}")
        print(df_ids_old)
        df_merged = df.merge(df_ids_old, left_on=row.chave_estrangeira, right_on='id_old_insert', how='left')
        print("feito o merge")
        df_merged['primary_id_query'] = df_merged['primary_id_query'].fillna(df[row.chave_estrangeira]).astype(int)
        print(df_merged)
        df_merged = df_merged.drop(columns=[row.chave_estrangeira])
        # df_merged = df_merged.rename(columns = {"primary_id_query": row.chave_estrangeira})
        df_merged = df_merged.rename(columns = {"primary_id_query": "test"})
        print("colunas dropada: "+ row.chave_estrangeira)
        print(df_merged)
        


def post_dados(cursor, tables_json, conn):
    global tables_finished
    while len(tables_finished) != total_tables:
        for table in tables_json:
            # try:
                if table["name_table"] not in tables_finished and verification_tables_finished(table["name_table"]):
                    if "not_primary_key" in table:
                        not_primary_key = table["not_primary_key"]
                    else:
                        not_primary_key = False
                    
                    tabela = table["name_table"]
                    if( not not_primary_key):
                        primaryKey = table["primary_key"]
                    unwanted_attributes = table["unwanted_attributes"]
                    
                    df = open_file_in_df(table)
                    get_new_id(df,tabela, cursor )
                    if( not not_primary_key):
                        df = df.rename(columns = {primaryKey:'id_old_insert'})
                    colunas = list(df.columns)

                    for nulls in unwanted_attributes:
                        if nulls in colunas:
                            colunas.remove(nulls)
                            
                    if( not not_primary_key):
                        cursor.execute(f"ALTER TABLE {tabela} ADD COLUMN (id_old_insert INT)")
                        conn.commit()

                    query = f"INSERT INTO {tabela} (`{'`, `'.join(colunas)}`) VALUES ({', '.join(['%s' for _ in colunas])})"
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
                    tables_finished.append(tabela)
                    
            # except Exception as e:
            #     print(f"Erro ao inserir dados na tabela {tabela}: {e}")



def insert_tables_not_relation(cursor, tables_json, conn, list_tables):
    global tables_finished
    for table in tables_json:
        try:
            if table["name_table"] in list_tables:
                if "not_primary_key" in table:
                    not_primary_key = table["not_primary_key"]
                else:
                    not_primary_key = False
                
                tabela = table["name_table"]
                if( not not_primary_key):
                    primaryKey = table["primary_key"]
                unwanted_attributes = table["unwanted_attributes"]
                
                df = open_file_in_df(table)
                if( not not_primary_key):
                    df = df.rename(columns = {primaryKey:'id_old_insert'})
                colunas = list(df.columns)

                for nulls in unwanted_attributes:
                    if nulls in colunas:
                        colunas.remove(nulls)
                        
                if( not not_primary_key):
                    cursor.execute(f"ALTER TABLE {tabela} ADD COLUMN (id_old_insert INT)")
                    conn.commit()

                query = f"INSERT INTO {tabela} (`{'`, `'.join(colunas)}`) VALUES ({', '.join(['%s' for _ in colunas])})"
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
                    
                tables_finished.append(tabela)

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
        global total_tables
        cursor, conn = db_conect(json)

        tebles_names = ["usuario","grupo","permissoes","rota"]#get_tables_name(cursor)
        total_tables = len(tebles_names)
        
        colunas_com_forengKey = ["permissoes","usuario"]#get_foreign_keys(cursor)
        get_foreign_keys(cursor)
        resultado = list(set(tebles_names) - set(colunas_com_forengKey))
        
        
        insert_tables_not_relation(cursor, json["tables"], conn, resultado)

        post_dados(cursor, json["tables"], conn)
        print(df_forengKey)

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
            "file_sep": ";",
            "autoIncrement":True
        },
        {
            "name_table": "grupo",
            "primary_key": "id",
            "unwanted_attributes": [],
            "path_file": "./grupos.csv",
            "type_file":"csv",
            "file_sep": ";",
            "autoIncrement":True
        },
        {
            "name_table": "permissoes",
            "not_primary_key": True,
            "unwanted_attributes": [],
            "path_file": "./permissoes.csv",
            "type_file":"csv",
            "file_sep": ",",
            "autoIncrement":False
        },
        {
            "name_table": "rota",
            "primary_key": "id",
            "unwanted_attributes": [],
            "path_file": "./rota.csv",
            "type_file":"csv",
            "file_sep": ",",
            "autoIncrement":True
        }
    ]
  })