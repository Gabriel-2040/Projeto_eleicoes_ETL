import json
import unicodedata

def remove_acentos(texto):
    return ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )

# Caminho do arquivo JSON
caminho_arquivo = r"E:\PYTHON\CURSOS\10_Digital_College\ETL\projeto etl\mapas ibge\AC_Municipios_2023\AC_Municipios_2023.json"

# Ler o arquivo JSON
with open(caminho_arquivo, encoding="utf-8") as arquivo:
    dados = json.load(arquivo)

# Função para remover acentos e converter para caixa alta apenas nas chaves específicas
def processar_json(obj):
    if isinstance(obj, dict):
        # Verificar se a chave é uma das que precisam de caixa alta
        for chave, valor in obj.items():
            if chave in ["NM_MUN"] and isinstance(valor, str):
                obj[chave] = remove_acentos(valor).upper()
            else:
                obj[chave] = processar_json(valor)
        return obj
    elif isinstance(obj, list):
        return [processar_json(item) for item in obj]
    else:
        return obj

# Processar o JSON (remover acentos e deixar valores de chaves específicas em caixa alta)
dados_processados = processar_json(dados)

# Salvar o JSON modificado de forma prettified
caminho_saida = r"E:\PYTHON\CURSOS\10_Digital_College\ETL\projeto etl\mapas ibge\AC_Municipios_2023\AC_tratado.json"
with open(caminho_saida, "w", encoding="utf-8") as arquivo_saida:
    json.dump(dados_processados, arquivo_saida, ensure_ascii=False, indent=4)

print(f"Arquivo salvo em: {caminho_saida}")
