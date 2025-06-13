import psycopg2
import pandas as pd

# Conectar ao PostgreSQL
def connect_db():
    return psycopg2.connect(
        host="localhost",  # Substitua pelo seu host
        port="5433",  # Substitua pelo seu porto (geralmente 5432)
        dbname="da_16_etl",  # Substitua pelo seu nome de banco de dados
        user="postgres",  # Substitua pelo seu nome de usuário
        password="1234"  # Substitua pela sua senha
    )

# Obter os dados do banco de dados
def get_data():
    conn = connect_db()
    query = """
    SELECT DISTINCT
        t.nr_cep,
        t.nr_latitude,
        t.nr_longitude,
        t.nr_zona,  -- Casting explícito para INTEGER
        t.cd_municipio,  -- Casting explícito para INTEGER
        t.sg_uf,
        t.aa_eleicao,
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_bairro, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_municipio, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
        da.id AS fk_id_ano
    FROM public.tabela_cep_2012 t
    INNER JOIN datawarehouse.dim_ano da ON da.ano = t.aa_eleicao
    UNION ALL
    SELECT DISTINCT
        t.nr_cep,
        t.nr_latitude,
        t.nr_longitude,
        t.nr_zona,  -- Casting explícito para INTEGER
        t.cd_municipio,  -- Casting explícito para INTEGER
        t.sg_uf,
        t.aa_eleicao,
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_bairro, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_municipio, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
        da.id AS fk_id_ano
    FROM public.tabela_cep_2016 t
    INNER JOIN datawarehouse.dim_ano da ON da.ano = t.aa_eleicao
    UNION ALL
    SELECT DISTINCT
        t.nr_cep,
        t.nr_latitude,
        t.nr_longitude,
        t.nr_zona,  -- Casting explícito para INTEGER
        t.cd_municipio,  -- Casting explícito para INTEGER
        t.sg_uf,
        t.aa_eleicao,
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_bairro, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_municipio, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
        da.id AS fk_id_ano
    FROM public.tabela_cep_2020 t
    INNER JOIN datawarehouse.dim_ano da ON da.ano = t.aa_eleicao;
    """

    # Carregar os dados em um DataFrame
    df = pd.read_sql(query, conn)

    conn.close()
    return df

# Função para criar uma nova tabela no PostgreSQL
def create_new_table(df):
    conn = connect_db()
    cur = conn.cursor()

    # Criar a nova tabela no banco de dados
    create_table_query = """
    CREATE TABLE IF NOT EXISTS datawarehouse.fato_cep (
        id SERIAL PRIMARY KEY,
        nr_cep INTEGER,
        nr_latitude NUMERIC,
        nr_longitude NUMERIC,
        nr_zona INTEGER,
        cd_municipio BIGINT,
        sg_uf CHARACTER VARYING (2),
        aa_eleicao BIGINT,
        nm_bairro CHARACTER VARYING (100),
        nm_municipio CHARACTER VARYING (100),
        fk_id_ano INTEGER
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    # Inserir os dados processados na nova tabela
    insert_query = """
    INSERT INTO datawarehouse.fato_cep (
        nr_cep, nr_latitude, nr_longitude, nr_zona, cd_municipio, sg_uf, aa_eleicao,
        nm_bairro, nm_municipio, fk_id_ano
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row['nr_cep'], row['nr_latitude'], row['nr_longitude'],
            row['nr_zona'], row['cd_municipio'], row['sg_uf'], row['aa_eleicao'], row['nm_bairro'],
            row['nm_municipio'], row['fk_id_ano']
        ))

    conn.commit()
    conn.close()

# Execução do fluxo
if __name__ == "__main__":
    # Etapa 1: Obter dados
    df = get_data()

    # Etapa 3: Criar e inserir na nova tabela
    create_new_table(df)

    print("Nova tabela criada e dados inseridos com sucesso!")


