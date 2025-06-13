# Projeto de Análise de Dados - Disciplina de ETL - Digital College  
### Professor Ronathan  
## Base de dados: TSE / IBGE  

### Entendimento do Negócio  
Esta base do TSE, referente às eleições para prefeito no Brasil nos anos de 2008, 2012, 2016 e 2020, foi fornecida pelo professor com todos os anos juntos, totalizando 12.117.886 linhas.  

A proposta deste trabalho é tirar conclusões sobre os partidos mais votados por ano, estado, zona eleitoral e verificar a evolução dos votos (se aumentaram ou diminuíram).  

Durante a disciplina, aprendemos a utilizar o Pentaho e o SQL, que são as ferramentas principais usadas para o tratamento dos dados.

---

## Etapa de Análise / Decisão  

O projeto se iniciou com a definição dos caminhos e objetivos da análise. A primeira etapa foi a criação de um modelo conceitual no brModelo, definindo as dimensões do Data Warehouse necessárias para responder às perguntas propostas.  

📁 Os arquivos dessa fase estão na pasta: `./_1_modelo_conceitual`  

### Modelo Conceitual  

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/brmodelo.JPG" alt="brmodelo" />
</p>

Foi decidido também que a análise incluiria mapas interativos por zona eleitoral. Para isso, era necessário complementar a base principal (`./_2_base_de_dados/eleicoes.backup`) com outras bases disponíveis no TSE que continham CEP, bairro, endereço, etc. Essas bases adicionais foram usadas para cruzar dados e enriquecer a visualização no Power BI.  

📁 Esses arquivos estão na pasta: `./_2_base_de_dados`

### Base de Dados  

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/base%20de%20dados.JPG" alt="base de dados" />
</p>

---

## Montagem do Data Warehouse | Etapa de ETL  

A etapa de ETL começou com o uso do Pentaho e SQL. O processo envolveu a montagem das dimensões e a verificação dos tipos de dados.

Um dos primeiros desafios foi identificar a codificação dos arquivos do TSE, que não estavam em UTF-8. Para isso, foi utilizado um script Python que detectava a codificação e o separador automaticamente.

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/python%20codigo%20separador.JPG" alt="python codigo separador" />
</p>

Com a codificação correta, foi possível utilizar o Pentaho para criar e manipular tabelas de forma prática. Porém, ao trabalhar com a base `eleicoes.backup`, as consultas e inserções ficaram muito lentas.  

🔧 Solução:  
- Usar o Pentaho apenas para visualização e tabelas menores  
- Utilizar SQL diretamente nas cargas maiores e criação da tabela fato  

📌 Foi necessário dividir a **tabela fato** em duas:  
- `fato_eleicao`  
- `fato_endereco`  

Isso reduziu a quantidade de *joins* durante a carga de dados e facilitou o uso de mapas.

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/tabelas%20fato.JPG" alt="tabelas fato" />
</p>

Outro desafio foi utilizar latitude e longitude nos mapas. Como nem todas as zonas eleitorais tinham coordenadas válidas, foi criada uma **dim_cep** com as colunas `id`, `cep`, `latitude`, `longitude`, mas muitos CEPs apareceram como `00000000`.  

🔍 Foram usadas APIs (BrazilCEP e Google Geocode) para preencher os dados ausentes.  
Ainda assim, a visualização não funcionou corretamente no Power BI.  

✅ Solução: uso de **arquivos GeoJSON** obtidos no site do IBGE, que permitiram a integração correta com os dados.

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/mapas%20json.JPG" alt="mapas json" />
</p>

---

## Respostas Iniciais  

Antes de migrar para o Power BI, foram criadas *views* no PostgreSQL para responder a algumas perguntas iniciais:

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/views.JPG" alt="views" />
</p>

📚 Aprendizado relevante:  
A cláusula `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` foi essencial para gerar rankings dentro de grupos de dados.

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/rank.JPG" alt="rank" />
</p>

---

## Power BI  

As visualizações foram construídas com base em consultas SQL das dimensões e tabelas fato. O Power BI se conectou ao PostgreSQL para gerar os painéis:

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/relacional_bi.JPG" alt="relacional_bi" />
</p>

### Páginas criadas:

#### 1 - Página Inicial  
- Navegação para Brasil / Região / Estados  
<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/eleicoes_01.JPG" alt="eleicoes_01" />
</p>

#### 2 - Brasil  
- Filtros: turno, ano  
- Métricas: votos por ano, partido, turno; partidos com mais prefeituras/votos  

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/eleicoes_02.JPG" alt="eleicoes_02" />
</p>

#### 3 - Região  
- Filtros: turno, ano, região  
- Métricas: votos por região, partido com mais votos por região  

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/eleicoes_03.JPG" alt="eleicoes_03" />
</p>

#### 4 - Estados  
- Filtros: turno, ano, estado  
- Métricas: partido mais votado por ano e turno, variações no número de votos  

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/eleicoes_04.JPG" alt="eleicoes_04" />
</p>

##### 4.1 - Estado Selecionado  
- Análise por turno  
- Visualização dos municípios com ou sem segundo turno  

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/eleicoes_05.JPG" alt="eleicoes_05" />
</p>

##### 4.2 - Município / Zonas Eleitorais *(Em desenvolvimento)*  
- Detalhamento por zona eleitoral  
- Filtros: turno, ano  

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/eleicoes_06.JPG" alt="eleicoes_06" />
</p>

---

## QGIS  

O QGIS foi utilizado para construir o painel de zonas eleitorais de Fortaleza. Como não foi possível encontrar um arquivo GeoJSON pronto, foi criada uma camada de polígonos manualmente sobre uma imagem de zonas eleitorais, e exportada para JSON com atributos que permitiram o vínculo com o banco de dados.

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/qgis.JPG" alt="qgis" />
</p>
