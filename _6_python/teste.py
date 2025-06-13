import googlemaps
import pandas as pd
import os
import psycopg2
import requests

# Tente obter a chave da API do Google Maps a partir da variável de ambiente
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')

# Verifique se a chave foi recuperada corretamente
if not GOOGLE_MAPS_API_KEY:
    print("Erro: a chave da API do Google Maps não foi fornecida corretamente.")
    # Se você não configurou a variável de ambiente, você pode inserir a chave diretamente aqui
    GOOGLE_MAPS_API_KEY = 'AIzaSyBzPwOG9wINShCvrxwJ5sd-W9EJAqRC13k'  # Substitua pela sua chave

# Configurações do banco de dados PostgreSQL
db_config = {
    "dbname": "da_16_etl",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5433",
}


def convert_csv_with_cep_to_latitude_longitude(df):
    """Função para adicionar latitude e longitude ao DataFrame usando o CEP."""
    return pd.concat(
        (
            df,
            df['CEP'].apply(lambda cell: pd.Series(convert_cep_to_latitude_longitude(cell), index=['lat', 'long']))
        ),
        axis=1
    )


def convert_cep_to_latitude_longitude(cep):
    """Converte o CEP para latitude e longitude usando o Google Maps."""
    if cep:
        gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)
        geocode_result = gmaps.geocode(address=cep, region="BR")
        if geocode_result:
            location = geocode_result[0]['geometry']['location']
            lat = location['lat']
            lng = location['lng']
            return lat, lng
    return None, None  # Retorna None se não conseguir obter os dados


def get_lat_lon_from_cep(cep=None):
    """Obtém a latitude e longitude de um CEP ou de todos os CEPs no banco de dados."""
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Se um CEP for passado, consulta só esse, caso contrário, consulta todos os CEPs da tabela
        if cep:
            cursor.execute("SELECT nr_cep FROM datawarehouse.dim_cep WHERE nr_cep = %s;", (cep,))
        else:
            cursor.execute("SELECT nr_cep FROM datawarehouse.dim_cep;")
        
        ceps = cursor.fetchall()

        for cep_tuple in ceps:
            cep = cep_tuple[0]  # Extraí o CEP da tupla
            lat, lon = convert_cep_to_latitude_longitude(cep)
            if lat is not None and lon is not None:
                # Insere no banco de dados apenas se lat e lon forem válidos
                cursor.execute(""" 
                    INSERT INTO datawarehouse.dim_cep_atualizado (nr_cep, latitude, longitude)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (nr_cep) DO NOTHING;
                """, (cep, lat, lon))
                conn.commit()
                print(f"CEP {cep} atualizado com Latitude: {lat}, Longitude: {lon}.")
            else:
                # Caso não consiga obter latitude e longitude
                with open("ceps_falhos.txt", "a") as log_file:
                    log_file.write(f"Não foi possível obter dados para o CEP {cep}\n")
                print(f"Não foi possível obter dados para o CEP {cep}.")
    except Exception as e:
        print("Erro:", e)
    finally:
        if conn:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    # Teste com um CEP específico, ou pode passar None para processar todos os CEPs
    #get_lat_lon_from_cep("37503130")  # Exemplo com um CEP
    get_lat_lon_from_cep()  # Descomente esta linha para processar todos os CEPs
