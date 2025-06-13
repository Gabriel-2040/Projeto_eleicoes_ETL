import requests
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

# Criar a tabela dim_cep_python
def create_table():
    conn = connect_db()
    cursor = conn.cursor()

    # SQL para criar a tabela
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS datawarehouse.dim_cep_python (
        nr_cep VARCHAR(10) PRIMARY KEY,
        latitude NUMERIC,
        longitude NUMERIC
    );
    """

    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

# Obter dados (CEPs) do banco de dados
def get_data():
    conn = connect_db()
    query = """
    SELECT
        nr_cep
    FROM  datawarehouse.dim_cep
    """
    
    # Usar pandas para executar a consulta e retornar os dados
    df = pd.read_sql(query, conn)
    conn.close()  # Fechar a conexão após a consulta
    return df['nr_cep'].tolist()

# Função para obter latitude e longitude com a API do Google
def get_coordinates(cep, api_key):
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={cep}&key={api_key}"
    response = requests.get(url)
    data = response.json()
    
    if data["status"] == "OK":
        latitude = data["results"][0]["geometry"]["location"]["lat"]
        longitude = data["results"][0]["geometry"]["location"]["lng"]
        return latitude, longitude
    else:
        return None

# Função para processar os CEPs e obter as coordenadas
def process_ceps(api_key):
    ceps = get_data()  # Obter os CEPs do banco de dados
    coordinates_list = []  # Lista para armazenar as coordenadas
    
    for cep in ceps:
        coords = get_coordinates(cep, api_key)
        if coords:
            coordinates_list.append((cep, coords[0], coords[1]))
        else:
            coordinates_list.append((cep, None, None))  # Caso as coordenadas não sejam encontradas
    
    # Exibir ou retornar os resultados (por exemplo, como DataFrame)
    df_coordinates = pd.DataFrame(coordinates_list, columns=["CEP", "Latitude", "Longitude"])
    return df_coordinates

# Função para inserir os dados na tabela dim_cep_python
def insert_data(df):
    conn = connect_db()
    cursor = conn.cursor()

    # Inserir os dados na tabela
    for index, row in df.iterrows():
        insert_sql = """
        INSERT INTO datawarehouse.dim_cep_python (nr_cep, latitude, longitude)
        VALUES (%s, %s, %s)
        ON CONFLICT (nr_cep) DO NOTHING;  -- Ignora se o CEP já existir
        """
        cursor.execute(insert_sql, (row['CEP'], row['Latitude'], row['Longitude']))

    conn.commit()
    cursor.close()
    conn.close()

# Exemplo de uso:
api_key = "Sua_Chave_de_API"  # Substitua pela sua chave de API do Google Maps

# Criação da tabela dim_cep_python
create_table()

# Processar os CEPs e obter as coordenadas
coordinates_df = process_ceps(api_key)

# Inserir os dados na tabela dim_cep_python
insert_data(coordinates_df)

# Exibir as coordenadas (latitude e longitude) dos CEPs
print(coordinates_df)
