
import psycopg2
import pandas as pd
import requests

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
        select  
        de.ds_endereco,
        dc.nr_cep,
        dsu.sg_uf,
        dm.nm_municipio
        from datawarehouse.dim_endereco de
        inner join datawarehouse.dim_cep as dc on dc.id = de.fk_nr_cep
        inner join datawarehouse.dim_sg_uf as dsu on dsu.id = de.fk_sg_uf
        inner join datawarehouse.dim_municipio as dm on dm.id = de.fk_nm_municipio
        WHERE LENGTH(nr_cep::TEXT) < 8
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Função para acessar a API e corrigir o CEP
def corrigir_cep(estado, municipio, endereco):
    url = f'https://viacep.com.br/ws/{estado}/{municipio}/{endereco}/json/'
    try:
        response = requests.get(url)
        data = response.json()
        if 'cep' in data and data['cep']:
            return data['cep']
        else:
            return None
    except Exception as e:
        print(f"Erro ao acessar a API para {municipio}, {estado}, {endereco}: {e}")
        return None

# Função para corrigir os CEPs na planilha
def corrigir_ceps_na_planilha(df):
    for index, row in df.iterrows():
        cep = str(row['nr_cep'])
        if len(cep) < 8 or not cep:
            estado = row['sg_uf']
            municipio = row['nm_municipio']
            endereco = row['ds_endereco']
            novo_cep = corrigir_cep(estado, municipio, endereco)
            if novo_cep:
                df.at[index, 'nr_cep'] = novo_cep
                print(f"CEP corrigido para {municipio}, {estado}, {endereco}: {novo_cep}")
            else:
                print(f"CEP não corrigido para {municipio}, {estado}, {endereco}")
    return df

# Executar o fluxo
if __name__ == "__main__":
    df = get_data()
    df_corrigida = corrigir_ceps_na_planilha(df)
    print("Nova tabela criada e dados inseridos com sucesso!")
    with pd.ExcelWriter('cep_corrigido.xlsx') as writer:
        df_corrigida.to_excel(writer, index=False)
        


