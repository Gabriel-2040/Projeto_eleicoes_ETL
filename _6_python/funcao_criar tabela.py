# Função para criar uma nova tabela no PostgreSQL
def create_new_table(df):
    conn = connect_db()
    cur = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS datawarehouse.endereço_novo(
        id SERIAL PRIMARY KEY,
        ds_endereco CHARACTER VARYING(100),
        nm_municipio CHARACTER VARYING(100),
        sg_uf CHARACTER VARYING(2),
        nr_cep INTEGER
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    insert_query = """
    INSERT INTO datawarehouse.endereço_novo (ds_endereco, nm_municipio, sg_uf, nr_cep)
    VALUES (%s, %s, %s, %s);
    """
    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row['ds_endereco'], row['nm_municipio'],
            row['sg_uf'], row['nr_cep']
        ))

    conn.commit()
    conn.close()