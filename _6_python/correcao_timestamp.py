import csv
from datetime import datetime

# Caminho do arquivo original e do novo arquivo corrigido
input_file = r"E:\PYTHON\CURSOS\10_Digital_College\ETL\projeto etl\zonas_estado\eleitorado_local_votacao_2024_corrigido.csv"
output_file = r"E:\PYTHON\CURSOS\10_Digital_College\ETL\projeto etl\zonas_estado\eleitorado_local_votacao_2024_corrigido_formatado.csv"

def correct_datetime_format(datetime_str):
    """Converte a data e hora de formato DD/MM/YYYY;HH:MM:SS para YYYY-MM-DD HH:MM:SS"""
    try:
        # Separa a data da hora, considerando o ponto e vírgula como delimitador
        date_part, time_part = datetime_str.split(';')
        # Converte a data de DD/MM/YYYY para YYYY-MM-DD
        corrected_date = datetime.strptime(date_part, '%d/%m/%Y').strftime('%Y-%m-%d')
        # Retorna a data e hora no formato esperado
        return f"{corrected_date} {time_part}"
    except ValueError:
        return datetime_str  # Retorna o valor original caso não seja uma data válida

# Criar o arquivo corrigido
with open(input_file, 'r', encoding='ISO-8859-1') as infile, open(output_file, 'w', encoding='ISO-8859-1', newline='') as outfile:
    reader = csv.reader(infile, delimiter=';')
    writer = csv.writer(outfile, delimiter=';', quoting=csv.QUOTE_ALL)
    
    # Lê o cabeçalho e escreve no novo arquivo
    header = next(reader)
    writer.writerow(header)
    
    # Itera sobre as linhas e corrige as datas
    for row in reader:
        # Corrige a coluna dt_geracao (index 0) e outras colunas de timestamp (ex: 2, 3, 4, etc)
        row[0] = correct_datetime_format(row[0])  # Exemplo de correção para dt_geracao
        # Repita para outras colunas de tipo timestamp, se necessário
        writer.writerow(row)

print(f"Arquivo corrigido salvo em: {output_file}")
