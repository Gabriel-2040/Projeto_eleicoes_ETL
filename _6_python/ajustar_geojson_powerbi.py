import json

def converter_geojson_para_json(arquivo_entrada, arquivo_saida):
    """
    Converte um arquivo GeoJSON para um JSON simples compatível com o Power BI.
    
    Parâmetros:
    - arquivo_entrada: Caminho para o arquivo GeoJSON de entrada.
    - arquivo_saida: Caminho para salvar o arquivo JSON ajustado.
    """
    try:
        # Abrir o arquivo GeoJSON de entrada
        with open(arquivo_entrada, 'r', encoding='utf-8') as f:
            dados = json.load(f)

        # Validar o formato do arquivo
        if dados.get("type") not in ["GeometryCollection", "FeatureCollection"]:
            raise ValueError("O arquivo não está no formato esperado (GeometryCollection ou FeatureCollection).")

        # Processar os dados e extrair as informações necessárias
        zonas = []
        for feature in dados.get("features", []):
            propriedades = feature.get("properties", {})
            geometria = feature.get("geometry", {})
            
            # Simplificar geometria para coordenadas apenas
            coordenadas = geometria.get("coordinates", [])
            
            # Montar estrutura simplificada
            zonas.append({
                "id": propriedades.get("id"),
                "nome_zona": propriedades.get("nr_zona"),
                "path": propriedades.get("path"),
                "coordenadas": coordenadas
            })

        # Salvar o resultado em JSON no formato esperado
        with open(arquivo_saida, 'w', encoding='utf-8') as f:
            json.dump(zonas, f, ensure_ascii=False, indent=4)

        print(f"Arquivo JSON compatível com Power BI salvo em: {arquivo_saida}")
    
    except FileNotFoundError:
        print(f"Erro: O arquivo '{arquivo_entrada}' não foi encontrado.")
    except json.JSONDecodeError:
        print("Erro: O arquivo fornecido não é um GeoJSON válido.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")

# Caminhos dos arquivos
arquivo_geojson = r"E:\PYTHON\CURSOS\10_Digital_College\ETL\projeto etl\QGIS\teste_zona.json.geojson"  # Substitua pelo caminho do seu arquivo GeoJSON
arquivo_json = r"E:\PYTHON\CURSOS\10_Digital_College\ETL\projeto etl\QGIS\zonas_fortaleza_tradado.json"       # Nome do arquivo de saída JSON

# Executar a conversão
converter_geojson_para_json(arquivo_geojson, arquivo_json)
