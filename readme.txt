# Projeto de Analise de Dados - Disciplina de ETL - Digital College
### Professor Ronathan
## Base de dados : TSE / IBGE
#### Entendimento do Negócio
Essa base do TSE, das eleições para prefeito no Brasil, no periodo que vai de 2008,2012,2016 e 2020. Abase original recebida pelo Professor
são todos os anos juntos e tem um total de 12117886 de linhas.
A proposta desse trabalho é tirar conclusões sobre os partidos mais votados, por ano, estado, zona, graduação de votos (se aumentou ou diminuiu).
Nessa disciplina aprendemos a utilizar o Pentaho e SQL que deverão ser as ferramentas principais para o tratamento dos dados.

## Etapa de Analise / Decisão.
- Esse projeto se iniciou com as decisões sobre que caminhos tomar e o que mostrar na análise. Em primeiro lugar foi montado
um modelo conceitual no brmodelo. Onde decidi que dimensões do datawarehouse criar para responder as minhas perguntas.
Os arquivos da base de dados se encontram na pasta ./_1_modelo_conceitual

###Modelo Conceitual

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/brmodelo.JPG" alt="brmodelo" />
</p>

- Nessa etapa também foi decido que gostaria de mostrar uma analise interativa, com mapas, para poder ter acesso a uma análise por zona
eleitoral e para isso era preciso ter dados que se interligassem com a tabela principal (".\_2_base_de_dados\eleicoes.backup")
A partir disso, em pesquisa se descobriu que existiam bases no TSE que continham cep, bairro, endereço, etc, que se une  com a base recebidae que
algumas colunas que não estavam na base atual poderiam complementar e ser usados no PowerBi. Dessa forma foi encontrado arquvivos no TSE com os anos correlatos 
a base recebida, com os campos que houvessem chave de ligação. 
Os arquivos se encontram na pasta ./_2_base_de_dados

###Base de Dados

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/base de dados.JPG" alt="base de dados" />
</p>


## Montagem Datawarehouse | Etapa de ETL
Nesse momento foi onde se começou a utilizar o pentaho e o SQl de fato. Na montagem das dimensões, verificação de tipos de dados.
Como proposta do curso o aprendizado pretendia se usar o Pentaho. Como primeira dificuldade surgiu a codificação das tabelas vindas do TSE,
que não eram UTF-8. Assim para não se precisar ficar testando uma por uma se usou um codgido em python para descobrir a codificação e 
o separador.

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/python codigo separador.JPG" alt="python codigo separador" />
</p>

Após a se saber a codificação foi usado o pentahoo para se fazer as dimensões. Muito prático na escolha dos dados e criação e manipulação das tabelas.
Um probema surgiu quando se começou a usar a inserção dos dados nas dimensões, com os dados que vinham da tabela (.\_2_base_de_dados\eleicoes.backup),
pois ele passou a demorar muito as consultas e inserçoes no banco. Assim uma das decisões foi usar ele como um visualizador e ser usado em tabelas menores,
e o uso em tabelas maiores e na sequencia da criação da tabela fato foi usado o sql mesmo.
Como passou-se a usar mais o pentaho se tornou (pesado) para a quantidade de movimentação de dados ficou sendo usado o sql.
A montagem da tabela fato gerou muitos problemas por conta da quantiade de dados, chaves estangeiras etc. Uma solução adotada foi fazer duas
tabelas fato:
    - fato_eleicao          |   - fato_endereço
        - id          | - id 
        - fk_aa_eleição          | - ds_endereco
        - fk_sg_uf          | - fk_id_cep
        - fk_id_municipio          |  - fk_id_municipio
        - fk_nr_zona          | - fk_sg_uf
        - fk_nr_turno          |  - fk_id_bairro
        - fk_nr_votavel          | - fk_id_zona
        - nm_votavel          | - fk_id_zona
        - qt_aptos          | - fk_id_zona
        - qt_comparecimentos          |  - fk_id_ano
        - qt_abstenções          |
        - qt_votos_nominais          |
        - qt_votos_nominais          |
        - dt_carga          |
    
Isso dominuiu a quantidade de joins na hora da carga nas tabelas e me resolveu o problema dos mapas também.

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/tabelas fato.JPG" alt="tabelas fato" />
</p>

Outra dificuldade  que surgiu foi que atributo usar para se montar um mapa interativo. O uso de latitude e longitude não deu certo pois nem todas as zonas
eleitorais das cidades tinham as latitudes certas.
Foi feita uma dimensão dim_cep com as colunas( id, cep, latitude e longitude) onde apreceram muitos ceps(00000000).
A partir disso tentei usar as APIs de cep. Usei a Brazilcep e a do Google (geocode), onde consegui preencher os dados restantes. Mesmo apos esse trabalho
na motagem do powerbi vi que o uso de latitude e longitude não dava certo. Assim se optou por um outro procedimento que era o uso de mapas geojson,
onde eu poderia inserir caracteristicas ao json que pudessem ser interpretadas pelo PowerBi e que fosse a mesma informação que servisse para interligar
com as tabelas vindas do banco de dados.
Os mapas foram adquiridos no site do IBGE.

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/mapas json.JPG" alt="mapas json" />
</p>

#### Respostas Iniciais
Antes de se levar os dados para o powerbi foram feitas algumas views na postgres para verificar e reponder as perguntas iniciais:

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/views.JPG" alt="views" />
</p>

Uma aprendizado muito legal nessa etapa foi a clausula "row_number / over /partition by / order by /rank" no postgres. Ela me proporciona atribuir um numero sequencial
único para cada linha a partir de um grupo de dados agrupados onde eu escolho a condição do ordenamento.
Essa instrução foi usada em outras views;

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/rank.JPG" alt="rank" />
</p>

#### PowerBi

As visualizações foram a partir das consultas SQL, das dimensões e tabelas fato. Assim, fiz a conexão do Powebi com o Postgres e 
comecei a minuplar e montar os paineis.
Assim ficou o relacional do powerbi:

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/relacional_bi.JPG" alt="relacional_bi" />
</p>

Os paineis ficaram nessa sequencia:
    1 - Pagina inicial
        Icones para a página : 
            1.1 - Brasil 
            1.2 - Região
            1.3 - Estados

    <p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/eleicoes_01.JPG" alt="eleicoes_01" />
    </p>

    2 - Página : Brasil - Pagina com dados nivel brasil(dados gerais).
            Filtros: turno, ano da eleição.
            Respostas : Numero de votos por Ano | Partido | turno
                        5 partidos com mais prefeituras
                        Partidos com mais votos em 1 turno
    
       <p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/eleicoes_02.JPG" alt="eleicoes_02" />
    </p>
        
    3 - Página : Região - Pagina com dados nivel das regiões do brasil(norte, nordeste, centroeste, sudeste, sul).
            Filtros: turno, ano da eleição, regiao
            Respostas : Numero de votos por região
                        Partido com mais votos na regiao.
    
      <p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/eleicoes_03.JPG" alt="eleicoes_03" />
    </p>

    4 - Página : Estados - Pagina com dados dos Estados.
            Filtros: turno, ano da eleição, seletor de estados
            Respostas : - Partido mais votado no estado p/ano e se houve segundo turno.
                        - Partidos com maior aumento e maior declinicio
            Aqui no seletor de estados pode ser dado um clique no estado, que será direcionado para uma pagina como estados e seus municipios.

 <p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/eleicoes_04.JPG" alt="eleicoes_04" />
    </p>

        4.1- Pagina : Estado(selecionado)
            Filtros: turno, ano da eleição
            Respostas : - Partido mais votado no estado p/ano e turno.
                        - Ao selecionar o turno mostra onde houve vencedor em 1 turno. Se no mapa  ficar sem cor é que houve segundo turno.
        
             <p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/eleicoes_05.JPG" alt="eleicoes_05" />
    </p>

            4.2 - Página Municipio : Zonas eleitorias ( Em desenvolvimento)
                  Filtros: turno, ano da eleição
                  Respostas: Aqui se chaga na útlima instância. Onde se verifica a quantidade de votos que cada partido teve por zona.
                              Pagina ainda em desenvolvimento.
       <p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/eleicoes_06.JPG" alt="eleicoes_06" />
    </p>

#### QGIS

O QGIS foi necessário para a montagem do ultimo painel que foi o das zonas eleitorais de fortaleza. O painel das zonas eleitorias.
Como nãotinha achado um mapa json  ou geojson com as zonas, peguei uma imagem das zonas eleitorais e cobri com poligonos no qgis,
e transformei em um json, atribuido valores aos poligonos de acordo com o banco de dados.

<p align="center">
  <img src="https://github.com/Gabriel-2040/Projeto_eleicoes_ETL/blob/main/_9_imagens/qgis.JPG" alt="qgis" />
</p>




