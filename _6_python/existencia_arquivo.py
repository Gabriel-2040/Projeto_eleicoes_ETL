import os

input_file = r"E:\PYTHON\CURSOS\10_Digital_College\ETL\projeto etl\zonas_estado\eleitorado_local_votacao_2024.csv"
if os.path.exists(input_file):
    print("O arquivo existe!")
else:
    print("O arquivo não foi encontrado.")
