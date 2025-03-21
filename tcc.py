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


def verification_tables_finished(table_name):
    df = df_forengKey[df_forengKey["tabela"] == table_name]
    list_tables_foreng = df["tabela_estrangeira"].unique().tolist()
    return set(list_tables_foreng).issubset(set(tables_finished))


def create_coll_id_old_in_tables(tables_json, cursor, conn):
    for table_json in tables_json:
        name_table = table_json["name_table"]
        not_primary_key = get_not_primary_key(table_json)
        
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{name_table}' 
            AND COLUMN_NAME = 'id_old_insert' 
            AND TABLE_SCHEMA = DATABASE()
        """)
        coluna_existe = cursor.fetchone()[0]

        if coluna_existe:
            cursor.execute(f"ALTER TABLE {name_table} DROP COLUMN id_old_insert")
            conn.commit()
        if( not not_primary_key and table_json["autoIncrement"]):
            cursor.execute(f"ALTER TABLE {name_table} ADD COLUMN id_old_insert INT")
            conn.commit()
        

def get_not_primary_key(table):
    not_primary_key = False
    if "not_primary_key" in table:
        not_primary_key = table["not_primary_key"]
    return not_primary_key
        

## continuar minha função que busca os dados no banco de dados e alterar os ids antigos pelos novos,
def get_new_id_from_db(df, table_name, cursor, not_primary_key, table_json):
    df_filter = df_forengKey[df_forengKey["tabela"] == table_name]
    for row in df_filter.itertuples(index=False):
        if not table_json["autoIncrement"] and not not_primary_key:
            continue
        ids_old = cursor.execute(f"""
                SELECT 
                    {row.id_tabela_estrangeira}, id_old_insert
                FROM 
                    {row.tabela_estrangeira}
                WHERE 
                    id_old_insert IS NOT NULL
            """) or cursor.fetchall()
        df_ids_old = pd.DataFrame(ids_old, columns=["primary_id_query", "id_old_insert"])
        df = df.merge(df_ids_old, left_on=row.chave_estrangeira, right_on='id_old_insert', how='left')
        df['primary_id_query'] = df['primary_id_query'].fillna(df[row.chave_estrangeira]).astype(int)
        df = df.drop(columns=[row.chave_estrangeira, 'id_old_insert'])\
                            .rename(columns = {"primary_id_query": row.chave_estrangeira})
    return df

# a aplicação esta com erro na hora de apagar as colunas  unwanted_attributes do df, o que acontece e que como a colunas não esta sendo apagada, na hora de fazer o insert tem mais colunas nos dados do que no header
def open_file_in_df(table_json, cursor, not_primary_key, unwanted_attributes, has_foregkey = False):
    type_file = table_json["type_file"]
    df = None
    
    if(type_file.upper() == "CSV"):
        df = pd.read_csv(table_json["path_file"], sep=table_json["file_sep"])
        df = df[sorted(df.columns)]
    elif(type_file.upper() == "PARQUET"):
        df = pd.read_parquet(table_json["path_file"])
        df = df[sorted(df.columns)]
    elif(type_file.upper() == "JSON"):
        df = pd.read_json(table_json["path_file"])
        df = df[sorted(df.columns)]
    else:
        raise TypeError(f'Tipo do arquivo "{type_file}" não é valido.') 
    if(has_foregkey):
        return get_new_id_from_db(df, table_json["name_table"], cursor, not_primary_key, table_json)
    print(unwanted_attributes)
    print(df)
    df = df.drop(columns=[unwanted_attributes])
    print(df)
    return df


def post_dados(cursor, tables_json, conn):
    global tables_finished
    while len(tables_finished) != total_tables:
        cursor.close()
        cursor = conn.cursor()
        print("abriu novamente o cursor")

        for table in tables_json:
            # try:
                if table["name_table"] not in tables_finished and verification_tables_finished(table["name_table"]):
                    not_primary_key = get_not_primary_key(table)
                    
                    tabela = table["name_table"]
                    if( not not_primary_key):
                        primaryKey = table["primary_key"]
                    unwanted_attributes = table["unwanted_attributes"]
                    
                    df = open_file_in_df(table, cursor, not_primary_key, unwanted_attributes,  has_foregkey=True)

                    if( not not_primary_key and table["autoIncrement"]):
                        df = df.rename(columns = {primaryKey:'id_old_insert'})
                    colunas = list(df.columns)

                    for nulls in unwanted_attributes:
                        if nulls in colunas:
                            colunas.remove(nulls)
                            
                    # if( not not_primary_key and table["autoIncrement"]):
                    #     cursor.execute(f"ALTER TABLE {tabela} ADD COLUMN (id_old_insert INT)")
                    #     conn.commit()

                    query = f"INSERT INTO {tabela} (`{'`, `'.join(colunas)}`) VALUES ({', '.join(['%s' for _ in colunas])})"
                    valores = [tuple(row) for row in df.itertuples(index=False, name=None)]
                    print(query)
                    print(valores[0])
                    
                    if not valores:
                        print(f"Nenhum dado para inserir na tabela {tabela}.")
                        continue
                    cursor.executemany(query, valores)
                    cursor.fetchall()
                    conn.commit()
                    print(tabela)

                    if cursor.rowcount > 0:
                        print(f"{cursor.rowcount} registros inseridos com sucesso na tabela {tabela}!")
                    else:
                        print(f"Nenhum registro foi inserido na tabela {tabela}. Verifique os dados.")
                    tables_finished.append(tabela)
                    
            # except Exception as e:
            #     print(f"Erro ao inserir dados na tabela {tabela}: {e}")


# tirar a validação que valida se tem chave primaria pois se não tem chave estrangeira é obritorio ter a primaria
def insert_tables_not_relation(cursor, tables_json, conn, list_tables):
    global tables_finished
    for table in tables_json:
        try:
            if table["name_table"] in list_tables:
                
                tabela = table["name_table"]
                primaryKey = table["primary_key"]
                unwanted_attributes = table["unwanted_attributes"]
                
                df = open_file_in_df(table, cursor, False, unwanted_attributes)
                if(table["autoIncrement"]):
                    df = df.rename(columns = {primaryKey:'id_old_insert'})
                colunas = list(df.columns)

                for nulls in unwanted_attributes:
                    if nulls in colunas:
                        colunas.remove(nulls)
                        
                # if(table["autoIncrement"]):
                #     cursor.execute(f"ALTER TABLE {tabela} ADD COLUMN (id_old_insert INT)")
                #     conn.commit()

                query = f"INSERT INTO {tabela} (`{'`, `'.join(colunas)}`) VALUES ({', '.join(['%s' for _ in colunas])})"
                valores = [tuple(row) for row in df.itertuples(index=False, name=None)]
                
                if not valores:
                    print(f"Nenhum dado para inserir na tabela {tabela}.")
                    continue

                cursor.executemany(query, valores)
                conn.commit()
                print(tabela)

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
        collation="utf8mb4_general_ci",
        connection_timeout=300
        )
        
        if conn.is_connected():
            return conn.cursor(), conn
        else:
            print("Não foi possível estabelecer a conexão com o DB.")
    except Error as e:
        print(f"Erro ao se conectar com o banco de dados: {e}")


def main(json):
    # try:
        global total_tables
        cursor, conn = db_conect(json)

        # tebles_names = ["usuario","grupo","permissoes","rota","not_a_i"]#get_tables_name(cursor)
        tebles_names = get_tables_name(cursor)
        total_tables = len(tebles_names)
        create_coll_id_old_in_tables(json["tables"], cursor, conn)
        
        #colunas_com_forengKey = ["permissoes","usuario"]#get_foreign_keys(cursor)
        colunas_com_forengKey = get_foreign_keys(cursor)
        get_foreign_keys(cursor)
        resultado = list(set(tebles_names) - set(colunas_com_forengKey))
        
        
        insert_tables_not_relation(cursor, json["tables"], conn, resultado)

        post_dados(cursor, json["tables"], conn)
        print(df_forengKey)

        cursor.close()

    # except Error as e:
    #     print(f"Erro ao conectar ou executar a consulta: {e}")
    # finally:
    #     if 'conn' in locals() and conn.is_connected():
    #         conn.close()
    #         print("Conexão encerrada.")

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
            "path_file": "./banco_completo/usuario.csv",
            "type_file":"csv",
            "file_sep": ",",
            "autoIncrement":True
        },
        {
            "name_table": "grupo",
            "primary_key": "id",
            "unwanted_attributes": [],
            "path_file": "./banco_completo/grupo.csv",
            "type_file":"csv",
            "file_sep": ",",
            "autoIncrement":True
        },
        {
            "name_table": "permissoes",
            "not_primary_key": True,
            "unwanted_attributes": [],
            "path_file": "./banco_completo/permissoes.csv",
            "type_file":"csv",
            "file_sep": ",",
            "autoIncrement":False
        },
        {
            "name_table": "rota",
            "primary_key": "id",
            "unwanted_attributes": [],
            "path_file": "./banco_completo/rota.csv",
            "type_file":"csv",
            "file_sep": ",",
            "autoIncrement":True
        }
        ,
        {
            "name_table": "not_a_i",
            "primary_key": "id",
            "unwanted_attributes": [],
            "path_file": "./banco_completo/not_a_i.csv",
            "type_file":"csv",
            "file_sep": ";",
            "autoIncrement":False
        }
        ,
        {
            "name_table": "aluno",
            "primary_key": "id",
            "unwanted_attributes": [],
            "path_file": "./banco_completo/aluno.csv",
            "type_file":"csv",
            "file_sep": ",",
            "autoIncrement":True
        }
        ,
        {
            "name_table": "arquivos",
            "primary_key": "id",
            "unwanted_attributes": [],
            "path_file": "./banco_completo/arquivos.json",
            "type_file":"json",
            "autoIncrement":True
        }
        ,
        {
            "name_table": "aula",
            "primary_key": "id",
            "unwanted_attributes": ["pdf_questoes","pdf_resolucao"],
            "path_file": "./banco_completo/aula.csv",
            "type_file":"csv",
            "file_sep": ",",
            "autoIncrement":True
        }
        ,
        {
            "name_table": "feito",
            "primary_key": "id",
            "unwanted_attributes": [],
            "path_file": "./banco_completo/feito.csv",
            "type_file":"csv",
            "file_sep": ",",
            "autoIncrement":True
        }
        ,
        {
            "name_table": "modulo",
            "primary_key": "id",
            "unwanted_attributes": [],
            "path_file": "./banco_completo/modulo.csv",
            "type_file":"csv",
            "file_sep": ",",
            "autoIncrement":True
        }
        ,
        {
            "name_table": "professor",
            "primary_key": "id",
            "unwanted_attributes": [],
            "path_file": "./banco_completo/professor.csv",
            "type_file":"csv",
            "file_sep": ",",
            "autoIncrement":True
        }
        ,
        {
            "name_table": "turma",
            "primary_key": "id",
            "unwanted_attributes": [],
            "path_file": "./banco_completo/turma.csv",
            "type_file":"csv",
            "file_sep": ",",
            "autoIncrement":True
        }
    ]
  })


# main({
#   "db": {
#     "host": "cloud.fslab.dev",
#     "user": "plataforma_matematica",
#     "password": "admin",
#     "database": "plataforma_matematica",
#     "port": 8806
#   },
#   "tables": [
#         {
#             "name_table": "usuario",
#             "primary_key": "id",
#             "unwanted_attributes": ['refresh_token'],
#             "path_file": "./usuarios.csv",
#             "type_file":"csv",
#             "file_sep": ";",
#             "autoIncrement":True
#         },
#         {
#             "name_table": "grupo",
#             "primary_key": "id",
#             "unwanted_attributes": [],
#             "path_file": "./grupos.csv",
#             "type_file":"csv",
#             "file_sep": ";",
#             "autoIncrement":True
#         },
#         {
#             "name_table": "permissoes",
#             "not_primary_key": True,
#             "unwanted_attributes": [],
#             "path_file": "./permissoes.csv",
#             "type_file":"csv",
#             "file_sep": ",",
#             "autoIncrement":False
#         },
#         {
#             "name_table": "rota",
#             "primary_key": "id",
#             "unwanted_attributes": [],
#             "path_file": "./rota.csv",
#             "type_file":"csv",
#             "file_sep": ",",
#             "autoIncrement":True
#         }
#         # ,
#         # {
#         #     "name_table": "not_a_i",
#         #     "primary_key": "id",
#         #     "unwanted_attributes": [],
#         #     "path_file": "./not_a_i.csv",
#         #     "type_file":"csv",
#         #     "file_sep": ";",
#         #     "autoIncrement":False
#         # }
#     ]
#   })