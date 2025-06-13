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

def get_data():
    conn = connect_db()
    query = """
    SELECT
        t.nr_cep,
        t.nr_latitude,
        t.nr_longitude,
        t.nm_bairro,
        cd_municipio,
        nm_municipio,
        sg_uf,
        t.aa_eleicao,
        da.id AS fk_id_ano
    FROM public.tabela_cep_2012 t
    INNER JOIN datawarehouse.dim_ano da ON da.ano = t.aa_eleicao
    UNION ALL
    SELECT
        t.nr_cep,
        t.nr_latitude,
        t.nr_longitude,
        t.nm_bairro,
        cd_municipio,
        nm_municipio,
        sg_uf,
        t.aa_eleicao,
        da.id AS fk_id_ano
    FROM public.tabela_cep_2016 t
    INNER JOIN datawarehouse.dim_ano da ON da.ano = t.aa_eleicao
    UNION ALL
    SELECT
        t.nr_cep,
        t.nr_latitude,
        t.nr_longitude,
        t.nm_bairro,
        cd_municipio,
        nm_municipio,
        sg_uf,
        t.aa_eleicao,
        da.id AS fk_id_ano
    FROM public.tabela_cep_2020 t
    INNER JOIN datawarehouse.dim_ano da ON da.ano = t.aa_eleicao;
    """

    # Carregar os dados em um DataFrame
    df = pd.read_sql(query, conn)

    conn.close()
    return df

# Função para comparar e processar os dados
def process_data(df):
    # Garantir que a coluna nr_cep seja do tipo string
    df['nr_cep'] = df['nr_cep'].astype(str)
    
    # Remover hífens e garantir que o CEP tenha no máximo 8 caracteres
    df['nr_cep'] = df['nr_cep'].str.replace('-', '').str.slice(0, 8)
    
    # Normalizar e formatar os nomes de bairro e outros campos
    # Remover espaços, caracteres especiais e acentuação
    df['bairro_normalizado'] = df['nm_bairro'].str.lower() \
        .str.replace(r'\W', '', regex=True)  # Remove caracteres não alfanuméricos
    
    # Remover espaços adicionais e caracteres indesejados
    df['bairro_normalizado'] = df['bairro_normalizado'].str.replace(' ', '') \
        .str.replace('º', '') \
        .str.replace('ª', '') \
        .str.replace('~', '') \
        .str.replace('#', '')
    
    # Agrupar dados por bairro, cep, zona e ano, incluindo todas as colunas necessárias
    grouped = df.groupby(['bairro_normalizado', 'nr_cep', 'fk_id_ano']).agg({
        'nr_latitude': 'first',  # ou outra lógica de agregação que desejar
        'nr_longitude': 'first',
        'nm_bairro': 'first',
        'cd_municipio': 'first',  # Certifique-se de incluir 'cd_municipio'
        'nm_municipio': 'first',  # E outras colunas necessárias
        'sg_uf': 'first'
    }).reset_index()

    return grouped


# Função para criar uma nova tabela no PostgreSQL
def create_new_table(df):
    conn = connect_db()
    cur = conn.cursor()

    # Criar a nova tabela no banco de dados
    create_table_query = """
    CREATE TABLE IF NOT EXISTS datawarehouse.dim_bairro_comparado (
        id SERIAL PRIMARY KEY,
        bairro_normalizado TEXT,
        nr_cep CHAR(8),
        fk_id_ano INTEGER,
        nr_latitude NUMERIC,
        nr_longitude NUMERIC,
        nm_bairro TEXT,
        cd_municipio INTEGER,
        nm_municipio TEXT,
        sg_uf CHAR(2),
        UNIQUE (bairro_normalizado, nr_cep, fk_id_ano)
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    # Inserir os dados processados na nova tabela
    insert_query = """
INSERT INTO datawarehouse.dim_bairro_comparado (
    bairro_normalizado, nr_cep, fk_id_ano, nr_latitude, nr_longitude, nm_bairro, cd_municipio, nm_municipio, sg_uf
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
"""
    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row['bairro_normalizado'], row['nr_cep'], row['fk_id_ano'],
            row['nr_latitude'], row['nr_longitude'], row['nm_bairro'], row['cd_municipio'],
            row['nm_municipio'], row['sg_uf']
        ))

    conn.commit()
    conn.close()

# Execução do fluxo
if __name__ == "__main__":
    # Etapa 1: Obter dados
    df = get_data()

    # Etapa 2: Processar os dados
    processed_df = process_data(df)

    # Etapa 3: Criar e inserir na nova tabela
    create_new_table(processed_df)

    print("Nova tabela criada e dados inseridos com sucesso!")
