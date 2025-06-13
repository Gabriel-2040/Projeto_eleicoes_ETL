import pandas as pd
import requests
from tqdm import tqdm

# Função para consultar a API do Google Maps
def corrigir_endereco(api_key, endereco, bairro, municipio, estado):
    query = f"{endereco},{bairro}, {municipio}, {estado}, Brasil"
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={query}&key={api_key}"
    
    response = requests.get(url).json()
    
    if response["status"] == "OK":
        resultado = response["results"][0]
        endereco_corrigido = resultado["formatted_address"]
        cep_corrigido = next((comp["long_name"] for comp in resultado["address_components"] if "postal_code" in comp["types"]), None)
        return endereco_corrigido, cep_corrigido
    return None, None

# Carregar a tabela
df = pd.read_csv(r"E:\PYTHON\CURSOS\10_Digital_College\ETL\projeto etl\python\cep_etapa_02.csv")
print("Dados carregados:")
print(df.head())

# Configuração da API Key
API_KEY = "AIzaSyBzPwOG9wINShCvrxwJ5sd-W9EJAqRC13k"

# Criar colunas para endereços corrigidos
df["Endereco Corrigido"] = ""
df["CEP Corrigido"] = ""

# Processar cada linha com barra de progresso
for i, row in tqdm(df.iterrows(), total=len(df), desc="Processando linhas"):
    endereco = row["ds_endereco"]
    bairro = row['nm_bairro']
    municipio = row["nm_municipio"]
    estado = row["sg_uf"]
    
    # Adicionar log de progresso
    print(f"Processando linha {i+1}/{len(df)}: {endereco}, {bairro}, {municipio}, {estado}")
    
    try:
        endereco_corrigido, cep_corrigido = corrigir_endereco(API_KEY, endereco, bairro, municipio, estado)
        df.at[i, "Endereco Corrigido"] = endereco_corrigido
        df.at[i, "CEP Corrigido"] = cep_corrigido
    except Exception as e:
        print(f"Erro ao processar linha {i+1}: {e}")

# Salvar os resultados
output_path = r"E:\PYTHON\CURSOS\10_Digital_College\ETL\projeto etl\python\cep_etapa_04_api.csv"
df.to_csv(output_path, index=False)
print(f"Arquivo salvo em: {output_path}")



