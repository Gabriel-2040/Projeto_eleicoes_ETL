import chardet
import csv

# Abrindo o arquivo em modo binário para detectar o encoding
with open(r'.\eleitorado_local_votacao_2012.csv', 'rb') as f:
    # Detecta o encoding usando chardet
    result = chardet.detect(f.read(1000))
    encoding = result['encoding']
    print(f'A codificação detectada é: {encoding}')

# Agora, descobrir o delimitador
with open(r'.\eleitorado_local_votacao_2012.csv', 'r', encoding=encoding) as f:
    # Usa o Sniffer para detectar o delimitador
    sample = f.read(1000)  # Lê uma amostra do arquivo
    delimiter = csv.Sniffer().sniff(sample).delimiter
    print(f'O delimitador detectado é: "{delimiter}"')
