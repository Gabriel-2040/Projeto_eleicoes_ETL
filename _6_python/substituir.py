import csv
import os

# Caminhos absolutos para os arquivos
input_file = r"E:\PYTHON\CURSOS\10_Digital_College\ETL\projeto etl\zonas_estado\eleitorado_local_votacao_2024.csv"
output_file = r"E:\PYTHON\CURSOS\10_Digital_College\ETL\projeto etl\zonas_estado\eleitorado_local_votacao_2024_corrigido.csv"

line_to_fix = 92042  # Número da linha problemática (começa em 1)

def fix_line(row):
    # Correção específica para a linha errada (exemplo: remover quebras de linha)
    corrected_row = [field.replace('\n', ' ') for field in row]
    return corrected_row

# Criando o diretório de saída se não existir
os.makedirs(os.path.dirname(output_file), exist_ok=True)

# Abrindo os arquivos
with open(input_file, 'r', encoding='ISO-8859-1') as infile, open(output_file, 'w', encoding='ISO-8859-1', newline='') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
    
    for line_number, row in enumerate(reader, start=1):
        # Verificando o número da linha
        if line_number % 1000 == 0:  # Print a cada 1000 linhas processadas
            print(f"Processando linha {line_number}")
        
        if line_number == line_to_fix:
            print(f"Corrigindo linha {line_number}: {row}")
            row = fix_line(row)
        
        # Escrevendo a linha (corrigida ou não)
        writer.writerow(row)

print(f"Arquivo corrigido salvo em: {output_file}")
