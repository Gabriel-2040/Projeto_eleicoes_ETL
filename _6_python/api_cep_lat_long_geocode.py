import psycopg2
import requests

# Configurações do banco de dados PostgreSQL
db_config = {
    "dbname": "da_16_etl",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5433",
}

# Chave da API do Google Geocoding
GOOGLE_API_KEY =  "AIzaSyBzPwOG9wINShCvrxwJ5sd-W9EJAqRC13k"

# Função para buscar latitude e longitude do Google Geocoding API
def get_lat_lon_from_cep(cep):
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={cep}&key={GOOGLE_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if "results" in data and len(data["results"]) > 0:
            location = data["results"][0]["geometry"]["location"]
            return location["lat"], location["lng"]
        else:
            print(f"API retornou resultados vazios para o CEP {cep}: {data}")
    return None, None

# Conexão ao banco de dados
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    # Criar a nova tabela se não existir
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS datawarehouse.dim_cep_atualizado (
            id SERIAL PRIMARY KEY,
            nr_cep VARCHAR(8) NOT NULL UNIQUE,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION
        );
    """)
    conn.commit()
    
    # Buscar todos os CEPs da tabela original
    cursor.execute("SELECT nr_cep FROM datawarehouse.dim_cep;")
    ceps = cursor.fetchall()
    
    for cep in ceps:
        lat, lon = get_lat_lon_from_cep(cep)
        if lat is not None and lon is not None:
            with open("ceps_falhos.txt", "a") as log_file:
                log_file.write(f"{cep}\n")
                continue
            # Inserir o CEP com latitude e longitude na nova tabela
            cursor.execute("""
                INSERT INTO datawarehouse.dim_cep_atualizado (nr_cep, latitude, longitude)
                VALUES (%s, %s, %s)
                ON CONFLICT (nr_cep) DO NOTHING;
            """, (cep, lat, lon))
            conn.commit()
            print(f"CEP {cep} atualizado com Latitude: {lat}, Longitude: {lon}.")
        else:
            print(f"Não foi possível obter dados para o CEP {cep}.")

except Exception as e:
    print("Erro:", e)
finally:
    if conn:
        cursor.close()
        conn.close()

