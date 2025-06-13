-- Logico_eleicoes: */

-- 1- 

CREATE TABLE datawarehouse.dim_turno (
    id_turno serial PRIMARY KEY,
    tipo serial
);

-- 2-

CREATE TABLE datawarehouse.dim_data_eleicao (
    id serial PRIMARY KEY UNIQUE,
    dia date,
    mes date,
    ano date
);

– SELECT DISTINCT
–   EXTRACT(YEAR FROM dt_eleicao):: INT AS ano,
–  EXTRACT(MONTH FROM dt_eleicao)::INT AS mes,
–    EXTRACT(DAY FROM dt_eleicao):: INT AS dia
– FROM "public".eleicoes;


-- 3-

CREATE TABLE datawarehouse.dim_dsc_eleicao (
    id serial PRIMARY KEY UNIQUE,
    nome varchar
);

-- 4-

CREATE TABLE datawarehouse.dim_cep (
 id_cep serial PRIMARY KEY,
cep integer
 NR_LATITUDE BIGINT,
 NR_LONGITUDE BIGINT
);

-- 5-

CREATE TABLE datawarehouse.dim_partidos (
    id_partido serial PRIMARY KEY,
    legenda integer
);

-- 6-

CREATE TABLE datawarehouse.dim_estado (
    id_estado serial PRIMARY KEY UNIQUE,
    nome varchar
);

-- 7-

CREATE TABLE datawarehouse.dim_municipio (
    id_municipio serial PRIMARY KEY UNIQUE,
    nome_mun varchar,
    cod_mun integer,
    fk_id_estado integer REFERENCES datawarehouse.dim_estado(id_estado)
);

-- 8-
 
CREATE TABLE datawarehouse.fato_eleicoes (
    id serial PRIMARY KEY UNIQUE,
    fk_id_turno integer REFERENCES datawarehouse.dim_turno(id_turno),
    fk_id_data integer REFERENCES datawarehouse.dim_data_eleicao(id_data) , 
	fk_id_local_votacao integer REFERENCES datawarehouse.dim_local_votacao(id_local_votacao) ,
    fk_id_nome_candidato integer REFERENCES datawarehouse.dim_nome_candidato(id_nome_candidato),
    qtd_votos integer,
	qtd_comparecimentos integer,
    qtd_abstencoes integer,
    qtd_aptos integer
);

-- 9-

CREATE TABLE datawarehouse.dim_zonas_eleitorais (
    id_zona serial PRIMARY KEY UNIQUE,
    numero_zona integer,
    fk_id_estado integer REFERENCES datawarehouse.dim_estado(id_estado) ,
    fk_id_municipio integer REFERENCES datawarehouse.dim_municipio(id_municipio)
);
-- 11-

CREATE TABLE datawarehouse.dim_local_votacao (
    id_local_votacao serial PRIMARY KEY,
    nome_local varchar,
    fk_id_logradouro integer REFERENCES datawarehouse.dim_logradouro(id_logradouro) ,
    fk_id_secao integer REFERENCES datawarehouse.dim_secao(id_secao),
    fk_id_cep integer references datawarehouse.dim_cep(id_cep),
    fk_id_municipio integer REFERENCES datawarehouse.dim_municipio(id_municipio) ,
	numero integer
);

-- 12 -

CREATE TABLE datawarehouse.dim_secao (
    id_secao serial PRIMARY KEY,
    numero_secao integer,
    fk_id_municipio integer REFERENCES datawarehouse.dim_municipio(id_municipio),
    fk_id_logradouro integer REFERENCES datawarehouse.dim_logradouro(id_logradouro)
);
-- 13 -

CREATE TABLE datawarehouse.dim_nome_candidato (
    id_nome_candidato serial PRIMARY KEY,
    nome_candidato varchar,
    fk_id_partido integer REFERENCES datawarehouse.dim_partidos(id_partido)
);

-- 23_11_2024
SELECT DISTINCT EXTRACT(YEAR FROM dt_eleicao) AS ano_eleicao,
nr_votavel as legenda
FROM public.eleicoes
order by legenda

ALTER TABLE datawarehouse.dim_cep
ADD COLUMN nr_latitude BIGINT,
ADD COLUMN nr_longitude BIGINT;

select nr_local_votacao,
nm_local_votacao,
nr_secao,
sg_uf
from public.eleicoes_novo
where nr_local_votacao = 1147
and sg_uf = 'BA'
and nm_local_votacao ilike 'escola%'
order by nm_local_votacao

select * 
from datawarehouse.dim_cep
order by nr_cep



 24/11/2024

ALTER TABLE public."tabela cep geral"
ALTER COLUMN nr_latitude TYPE double precision USING nr_latitude::double precision;

ALTER TABLE public."tabela cep geral"
ALTER COLUMN nr_longitude TYPE double precision USING nr_longitude::double precision;

ALTER TABLE public."tabela cep geral"
ALTER COLUMN nm_municipio TYPE character varying(50);

ALTER TABLE public."tabela cep geral"
ALTER COLUMN nm_local_votacao TYPE character varying(100);

ALTER TABLE public."tabela cep geral"
ALTER COLUMN nm_local_votacao_original TYPE character varying(100);

ALTER TABLE public."tabela cep geral"
ALTER COLUMN ds_endereco TYPE character varying(100);

ALTER TABLE public."tabela cep geral"
ALTER COLUMN ds_endereco_locvt_original TYPE character varying(100);

ALTER TABLE public."tabela cep geral"
ALTER COLUMN nm_bairro TYPE character varying(100);



COPY public."tabela cep geral" (
    hh_geracao, aa_eleicao, dt_eleicao, ds_eleicao, nr_turno, sg_uf, cd_municipio, nm_municipio, 
    nr_zona, nr_secao, cd_tipo_secao_agregada, ds_tipo_secao_agregada, nr_secao_principal, nr_local_votacao, 
    nm_local_votacao, cd_tipo_local, ds_tipo_local, ds_endereco, nm_bairro, nr_cep, nr_telefone_local, 
    nr_latitude, nr_longitude, cd_situ_local_votacao, ds_situ_local_votacao, cd_situ_zona, ds_situ_zona, 
    cd_situ_secao, ds_situ_secao, cd_situ_localidade, ds_situ_localidade, cd_situ_secao_acessibilidade, 
    ds_situ_secao_acessibilidade, qt_eleitor_secao, qt_eleitor_eleicao_federal, qt_eleitor_eleicao_estadual, 
    qt_eleitor_eleicao_municipal, nr_local_votacao_original, nm_local_votacao_original, ds_endereco_locvt_original
)
FROM 'E:\PYTHON\CURSOS\10_Digital_College\ETL\projeto etl\zonas_estado\eleitorado_local_votacao_2024_corrigido_formatado.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ';',
    ENCODING 'ISO-8859-1',
    QUOTE '"',
    ESCAPE ''''
);

select DISTINCT
 nr_zona, cd_municipio, nm_municipio, sg_uf, nr_cep, nr_latitude, nr_longitude, "tabela_cep_2024".aa_eleicao
 from public."tabela_cep_2024"
order by sg_uf

select distinct aa_eleicao from public."tabela_cep_2024"


SELECT DISTINCT ds_eleicao from public.eleicoes
SELECT * from public.eleicoes
where ds_eleicao = 'Eleições Municipais 2020 - AP'

-- Inicialmente programadas para os dias 4 e 25 de outubro, 
-- as eleições municipais em todo o país foram transferidas para
-- 15 de novembro (primeiro turno) e 29 de novembro (segundo turno)
-- devido à pandemia de COVID-19. Porém, uma crise elétrica no estado 
-- do Amapá, que começou na primeira semana de novembro, obrigou a um novo adiamento 
-- das eleições para 6 e 20 de dezembro, respectivamente, por
-- falta de condições de segurança, materiais e técnicas. 

SELECT * from datawarehouse.dim_data_eleicao
order by ano

UPDATE datawarehouse.dim_ds_eleicao
SET ds_eleicao = 'eleicao municipal 2012'  
WHERE ds_eleicao = 'ELEIÇÃO MUNICIPAL 2012'; 

UPDATE datawarehouse.dim_ds_eleicao
SET ds_eleicao = 'eleicao municipais 2016'  
WHERE ds_eleicao = 'Eleicões Municipais 2016'; 

UPDATE datawarehouse.dim_ds_eleicao
SET ds_eleicao = 'eleicao municipais 2020'  
WHERE ds_eleicao = 'Eleicões Municipais 2020'; 

UPDATE datawarehouse.dim_ds_eleicao
SET ds_eleicao = 'eleicao municipais 2020 - ap'  
WHERE ds_eleicao = 'Eleicões Municipais 2020 - AP'; 

select * from datawarehouse.dim_ds_eleicao

CREATE EXTENSION IF NOT EXISTS unaccent;

UPDATE datawarehouse.dim_municipio
SET nm_municipio = lower(replace(unaccent(nm_municipio), 'ç', 'c'));

ALTER TABLE datawarehouse.dim_municipio
ADD COLUMN fk_id_sg_uf INT;

UPDATE datawarehouse.dim_municipio 
SET fk_id_sg_uf = subquery.id
FROM (
    SELECT 
        dm.cd_municipio,
        su.id
    FROM 
        datawarehouse.dim_municipio dm
    INNER JOIN public.eleicoes pe ON pe.cd_municipio = dm.cd_municipio
    INNER JOIN datawarehouse.dim_sg_uf su ON pe.sg_uf = su.sg_uf
) AS subquery
WHERE datawarehouse.dim_municipio.cd_municipio = subquery.cd_municipio;


SELECT * FROM datawarehouse.dim_municipio
where nm_municipio ilike 'sao%'

SELECT * FROM datawarehouse.dim_sg_uf
where dim_sg_uf.id = 51

ALTER TABLE datawarehouse.dim_municipio
ADD CONSTRAINT fk_dim_municipio_sg_uf
FOREIGN KEY (fk_id_sg_uf)
REFERENCES datawarehouse.dim_sg_uf (id)
ON DELETE CASCADE;



--adiconar fk no nomce candidato da dim_eleicao
ALTER TABLE datawarehouse.dim_nome_candidato
ADD COLUMN fk_id_dim_legenda INT;

UPDATE datawarehouse.dim_nome_candidato
SET fk_id_dim_legenda = subquery.id
FROM (select distinct
		dl.id,
		dnc.legenda,
		dl.nr_votavel
		FROM datawarehouse.dim_legenda dl
		inner join datawarehouse.dim_nome_candidato as dnc on dnc.legenda = dl.nr_votavel
	)as subquery
WHERE datawarehouse.dim_nome_candidato.legenda = subquery.legenda;

UPDATE datawarehouse.dim_nome_candidato
SET fk_id_dim_legenda = NULL;

UPDATE datawarehouse.dim_nome_candidato
SET legenda = NULL;

UPDATE datawarehouse.dim_nome_candidato dnc
SET legenda = subquery.nr_votavel
FROM (
    SELECT DISTINCT
        pe.nr_votavel,
        dnc.nm_votavel
    FROM public.eleicoes pe
    INNER JOIN datawarehouse.dim_nome_candidato dnc ON dnc.nm_votavel = pe.nm_votavel
) AS subquery
WHERE dnc.nm_votavel = subquery.nm_votavel;

select distinct nr_votavel from public.eleicoes

ALTER TABLE datawarehouse.dim_nome_candidato
ADD CONSTRAINT fk_dim_legenda
FOREIGN KEY (fk_id_dim_legenda)
REFERENCES datawarehouse.dim_legenda (id)
ON DELETE CASCADE;




27/11/2024

select distinct
nr_zona,
dsu.id as fk_id_sgu_uf,
dsu.sg_uf as estado,
dm.id as fk_id_municipio,
dm.nm_municipio as municipio

from public.eleicoes as pe
inner join datawarehouse.dim_municipio as dm on dm.cd_municipio = pe.cd_municipio
inner join datawarehouse.dim_sg_uf as dsu on dsu.sg_uf = pe.sg_uf
order by estado

CREATE TABLE datawarehouse.dim_zonas_eleitorais (
    id SERIAL PRIMARY KEY,
    fk_id_sgu_uf INTEGER NOT NULL REFERENCES datawarehouse.dim_sg_uf (id),
    fk_id_municipio INTEGER NOT NULL REFERENCES datawarehouse.dim_municipio (id),
    nr_zona INTEGER NOT NULL,
    municipio TEXT NOT NULL,
    estado CHAR(2) NOT NULL
);

INSERT INTO datawarehouse.dim_zonas_eleitorais (fk_id_sgu_uf, fk_id_municipio, nr_zona, municipio, estado)
SELECT DISTINCT
    dsu.id AS fk_id_sgu_uf,
    dm.id AS fk_id_municipio,
    pe.nr_zona,
    dm.nm_municipio AS municipio,
    dsu.sg_uf AS estado
FROM public.eleicoes AS pe
INNER JOIN datawarehouse.dim_municipio AS dm ON dm.cd_municipio = pe.cd_municipio
INNER JOIN datawarehouse.dim_sg_uf AS dsu ON dsu.sg_uf = pe.sg_uf
ORDER BY estado;

CREATE TABLE datawarehouse.dim_logradouro (
    id serial PRIMARY KEY UNIQUE,
    logradouro varchar,
    fk_id_municipio integer REFERENCES datawarehouse.dim_municipio(id)
);

-- enderço contem nulos
select distinct 
pe.ds_local_votacao_endereco as endereco,
dsu.sg_uf AS estado,
dm.nm_municipio AS municipio

FROM public.eleicoes AS pe
INNER JOIN datawarehouse.dim_municipio AS dm ON dm.cd_municipio = pe.cd_municipio
INNER JOIN datawarehouse.dim_sg_uf AS dsu ON dsu.sg_uf = pe.sg_uf
ORDER BY endereco;

dsu.id AS fk_id_sgu_uf,
dm.id AS fk_id_municipio,
dze.nr_zona
INNER JOIN datawarehouse.dim_zonas_eleitorais AS dze ON dze.nr_zona = pe.nr_zona

-- fazer o selct dos ceps
-- fazendo um union 
SELECT DISTINCT
    sg_uf,
    cd_municipio,
    nm_municipio,
    nr_zona,
    nm_local_votacao,
    nr_secao,
    ds_endereco,
    nm_bairro,
    nr_cep,
    nr_latitude,
    nr_longitude
FROM public.tabela_cep_2020

UNION

SELECT DISTINCT
    sg_uf,
    cd_municipio,
    nm_municipio,
    nr_zona,
    nm_local_votacao,
    nr_secao,
    ds_endereco,
    nm_bairro,
    nr_cep,
    nr_latitude,
    nr_longitude
FROM public.tabela_cep_2016

UNION

SELECT DISTINCT
    sg_uf,
    cd_municipio,
    nm_municipio,
    nr_zona,
    nm_local_votacao,
    nr_secao,
    ds_endereco,
    nm_bairro,
    nr_cep,
    nr_latitude,
    nr_longitude
FROM public.tabela_cep_2012;

-- comparacao para saber se  houve modificaçao no cep

SELECT
    sg_uf,
    cd_municipio,
    nm_municipio,
    nr_zona,
    nm_local_votacao,
    nr_secao,
    ds_endereco,
    nm_bairro,
    nr_cep,
    nr_latitude,
    nr_longitude,
    '2020' AS ano
FROM public.tabela_cep_2020

UNION ALL

SELECT
    sg_uf,
    cd_municipio,
    nm_municipio,
    nr_zona,
    nm_local_votacao,
    nr_secao,
    ds_endereco,
    nm_bairro,
    nr_cep,
    nr_latitude,
    nr_longitude,
    '2016' AS ano
FROM public.tabela_cep_2016

UNION ALL

SELECT
    sg_uf,
    cd_municipio,
    nm_municipio,
    nr_zona,
    nm_local_votacao,
    nr_secao,
    ds_endereco,
    nm_bairro,
    nr_cep,
    nr_latitude,
    nr_longitude,
    '2012' AS ano
FROM public.tabela_cep_2012;

SELECT
    COALESCE(a.sg_uf, b.sg_uf) AS sg_uf,
    COALESCE(a.cd_municipio, b.cd_municipio) AS cd_municipio,
    COALESCE(a.nm_municipio, b.nm_municipio) AS nm_municipio,
    a.nr_cep AS cep_2020,
    b.nr_cep AS cep_2016,
    CASE
        WHEN a.nr_cep IS DISTINCT FROM b.nr_cep THEN 'CEP Alterado'
        WHEN a.nr_cep IS NULL THEN 'CEP Removido'
        WHEN b.nr_cep IS NULL THEN 'CEP Adicionado'
        ELSE 'Sem Alterações'
    END AS status
FROM public.tabela_cep_2020 a
FULL OUTER JOIN public.tabela_cep_2016 b
ON a.cd_municipio = b.cd_municipio
   AND a.nr_zona = b.nr_zona
   AND a.nm_local_votacao = b.nm_local_votacao;

--fazer dimensões cep por ano
-- criar tablea cep 2020
CREATE TABLE datawarehouse.dim_cep_2020 (
    id SERIAL PRIMARY KEY,
    fk_id_ano INTEGER NOT NULL REFERENCES datawarehouse.dim_ano(id),
    fk_id_nr_zona INTEGER NOT NULL REFERENCES datawarehouse.dim_zonas_eleitorais(id),
    nr_cep CHAR(8) NOT NULL,
    nr_latitude NUMERIC,
    nr_longitude NUMERIC
);


-- Inserir os dados de 2020
INSERT INTO datawarehouse.dim_cep_2020 (
    fk_id_ano,
    fk_id_nr_zona,
    nr_cep,
    nr_latitude,
    nr_longitude
)
SELECT distinct
    da.id AS fk_id_ano,
    dze.id AS fk_id_nr_zona,
    tc2020.nr_cep,
    tc2020.nr_latitude,
    tc2020.nr_longitude
FROM public.tabela_cep_2020 tc2020
INNER JOIN datawarehouse.dim_ano da ON da.ano = tc2020.aa_eleicao
INNER JOIN datawarehouse.dim_zonas_eleitorais dze ON dze.nr_zona = tc2020.nr_zona;

-- criar tablea cep 2016
CREATE TABLE datawarehouse.dim_cep_2016 (
    id SERIAL PRIMARY KEY,
    fk_id_ano INTEGER NOT NULL REFERENCES datawarehouse.dim_ano(id),
    fk_id_nr_zona INTEGER NOT NULL REFERENCES datawarehouse.dim_zonas_eleitorais(id),
    nr_cep CHAR(8) NOT NULL,
    nr_latitude NUMERIC,
    nr_longitude NUMERIC
);


-- Inserir os dados de 2016
INSERT INTO datawarehouse.dim_cep_2016 (
    fk_id_ano,
    fk_id_nr_zona,
    nr_cep,
    nr_latitude,
    nr_longitude
)
SELECT distinct
    da.id AS fk_id_ano,
    dze.id AS fk_id_nr_zona,
    tc2016.nr_cep,
    tc2016.nr_latitude,
    tc2016.nr_longitude
FROM public.tabela_cep_2016 tc2016
INNER JOIN datawarehouse.dim_ano da ON da.ano = tc2016.aa_eleicao
INNER JOIN datawarehouse.dim_zonas_eleitorais dze ON dze.nr_zona = tc2016.nr_zona;

-- criar tabela cep 2012
CREATE TABLE datawarehouse.dim_cep_2012 (
    id SERIAL PRIMARY KEY,
    fk_id_ano INTEGER NOT NULL REFERENCES datawarehouse.dim_ano(id),
    fk_id_nr_zona INTEGER NOT NULL REFERENCES datawarehouse.dim_zonas_eleitorais(id),
    nr_cep CHAR(8) NOT NULL,
    nr_latitude NUMERIC,
    nr_longitude NUMERIC
);


-- Inserir os dados de 2012
INSERT INTO datawarehouse.dim_cep_2012 (
    fk_id_ano,
    fk_id_nr_zona,
    nr_cep,
    nr_latitude,
    nr_longitude
)
SELECT distinct
    da.id AS fk_id_ano,
    dze.id AS fk_id_nr_zona,
    tc2012.nr_cep,
    tc2012.nr_latitude,
    tc2012.nr_longitude
FROM public.tabela_cep_2012 tc2012
INNER JOIN datawarehouse.dim_ano da ON da.ano = tc2012.aa_eleicao
INNER JOIN datawarehouse.dim_zonas_eleitorais dze ON dze.nr_zona = tc2012.nr_zona;

select count (id) from datawarehouse.dim_cep_2012
select count (id) from datawarehouse.dim_cep_2016
select count (id) from datawarehouse.dim_cep_2020

select * from datawarehouse.dim_zonas_eleitorais
where municipio = 'sao paulo'
order by  estado, municipio, nr_zona


-- criar dimensão ano
CREATE TABLE datawarehouse.dim_ano (
    id SERIAL PRIMARY KEY,
    ano INTEGER NOT NULL
);
insert into datawarehouse.dim_ano(ano)
select distinct ano
from datawarehouse.dim_data_eleicao

ALTER TABLE datawarehouse.dim_data_eleicao
ADD COLUMN fk_id_dim_ano INTEGER REFERENCES datawarehouse.dim_ano(id);

UPDATE datawarehouse.dim_data_eleicao dde
SET fk_id_dim_ano = da.id
FROM datawarehouse.dim_ano da
WHERE dde.ano = da.ano;

ALTER TABLE datawarehouse.dim_data_eleicao
DROP COLUMN ano;

-- criar dimensão mes
CREATE TABLE datawarehouse.dim_mes (
    id SERIAL PRIMARY KEY,
    mes INTEGER NOT NULL
);
insert into datawarehouse.dim_mes(mes)
select distinct mes
from datawarehouse.dim_data_eleicao

ALTER TABLE datawarehouse.dim_data_eleicao
ADD COLUMN fk_id_dim_mes INTEGER REFERENCES datawarehouse.dim_mes(id);

UPDATE datawarehouse.dim_data_eleicao dde
SET fk_id_dim_mes = dm.id
FROM datawarehouse.dim_mes dm
WHERE dde.mes = dm.mes;

ALTER TABLE datawarehouse.dim_data_eleicao
DROP COLUMN mes;

29_11_2024
SELECT
    (SELECT COUNT(*) FROM datawarehouse.dim_cep_2012) AS total_tabela1,
    (SELECT COUNT(*) FROM datawarehouse.dim_cep_2016) AS total_tabela2,
    (SELECT COUNT(*) FROM datawarehouse.dim_cep_2016) AS total_tabela3;

ALTER TABLE datawarehouse.dim_cep_2012
 ALTER COLUMN nr_cep TYPE VARCHAR(9);
 ALTER TABLE datawarehouse.dim_cep_2016
 ALTER COLUMN nr_cep TYPE VARCHAR(9);

ALTER TABLE datawarehouse.dim_cep_2020
ALTER COLUMN nr_cep TYPE VARCHAR(9);
 
create table datawarehouse.dim_bairro
 (id serial primary key,
 nm_bairro varchar (70))
 
INSERT INTO datawarehouse.dim_bairro (nm_bairro)
SELECT DISTINCT nm_bairro
FROM datawarehouse.dim_bairro_comparado;

select * FROM public.tabela_cep_2020
WHERE nm_bairro is null

ALTER TABLE datawarehouse.dim_bairro_comparado
ADD COLUMN fk_id_municipio INTEGER REFERENCES datawarehouse.dim_municipio(id);

insert into datawarehouse.dim_bairro_comparado (fk_id_municipio)
select
	dm.id
from datawarehouse.dim_municipio dm
inner join datawarehouse.dim_bairro_comparado as dbc on dbc.nr_cep = dm.cep 

create table datawarehouse.dim_cep_lt_lg
(id serial primary key,
cep char (9),
latitude numeric,
longitue numeric);
CREATE TABLE datawarehouse.dim_cep_lt_lg (
    id SERIAL PRIMARY KEY,
    cep CHAR(9),
    latitude NUMERIC,
    longitude NUMERIC
);

INSERT INTO datawarehouse.dim_cep_lt_lg (cep, latitude, longitude)
SELECT DISTINCT nr_cep, nr_latitude, nr_longitude
FROM datawarehouse.dim_cep_2012
UNION
SELECT DISTINCT nr_cep, nr_latitude, nr_longitude
FROM datawarehouse.dim_cep_2016
UNION
SELECT DISTINCT nr_cep, nr_latitude, nr_longitude
FROM datawarehouse.dim_cep_2020;

ALTER TABLE datawarehouse.dim_bairro_comparado
ADD COLUMN fk_id_cep_lt_lg INT;

ALTER TABLE datawarehouse.dim_bairro_comparado
ADD CONSTRAINT fk_bairro_cep
FOREIGN KEY (fk_id_cep_lt_lg) REFERENCES datawarehouse.dim_cep_lt_lg(id);

-- Passo 2: Atualizar a chave estrangeira com base em nr_cep, nr_latitude e nr_longitude
UPDATE datawarehouse.dim_bairro_comparado dbc
SET fk_id_cep_lt_lg = dcl.id
FROM datawarehouse.dim_cep_lt_lg dcl
WHERE dbc.nr_cep = dcl.cep
  AND dbc.nr_latitude = dcl.latitude
  AND dbc.nr_longitude = dcl.longitude;

30_11_2024
ALTER TABLE datawarehouse.dim_bairro_comparado
ADD COLUMN fk_id_municipio INTEGER,
ADD CONSTRAINT fk_id_municipio
    FOREIGN KEY (fk_id_municipio) 
    REFERENCES datawarehouse.dim_municipio(id);

UPDATE datawarehouse.dim_bairro_comparado dbc
SET fk_id_municipio = dm.id
FROM datawarehouse.dim_municipio dm
WHERE  dm.nm_municipio =dbc.nm_municipio;

UPDATE datawarehouse.dim_bairro_comparado 
SET nm_municipio = lower(replace(unaccent(nm_municipio), 'ç', 'c')) 
WHERE nm_municipio is not null 

UPDATE datawarehouse.dim_municipio
SET nm_municipio = LOWER(REPLACE(UNACCENT(REPLACE(nm_municipio, 'ç', 'c')), '''', ''))
WHERE nm_municipio IS NOT NULL;

select distinct nm_municipio from datawarehouse.dim_bairro_comparado
where fk_id_municipio is null

ALTER TABLE datawarehouse.dim_bairro_comparado 
DROP COLUMN nm_municipio;

ALTER TABLE datawarehouse.dim_bairro_comparado 
DROP COLUMN cd_municipio;

UPDATE datawarehouse.dim_bairro_comparado dbc
SET fk_id_sg_uf = dsu.id
FROM datawarehouse.dim_sg_uf dsu
WHERE dbc.fk_id_sg_uf = dsu.sg_uf;

select fk_id_sg_uf from datawarehouse.dim_bairro_comparado
where fk_id_sg_uf is null

ALTER TABLE datawarehouse.dim_bairro_comparado
ADD COLUMN fk_id_cep INTEGER,
ADD CONSTRAINT fk_id_cep
    FOREIGN KEY (fk_id_cep) 
    REFERENCES datawarehouse.dim_cep_lt_lg(id);

UPDATE datawarehouse.dim_bairro_comparado dbc
SET fk_id_cep = ddc.id
FROM datawarehouse.dim_cep_lt_lg ddc
WHERE ddc.cep = dbc.nr_cep
and ddc.latitude = dbc.nr_latitude
and ddc.longitude = dbc.nr_longitude

select distinct nr_cep, nr_latitude, nr_longitude from datawarehouse.dim_bairro_comparado
where nr_cep = '0.0'

select distinct cep, latitude, longitude from datawarehouse.dim_cep_lt_lg
where cep = '0'

select distinct cep  from datawarehouse.dim_cep_lt_lg
where cep = '0'

update datawarehouse.dim_bairro_comparado
set nr_cep = '0'
where nr_cep = '0.0'

ALTER TABLE datawarehouse.dim_bairro_comparado 
DROP COLUMN nr_cep;

ALTER TABLE datawarehouse.dim_bairro_comparado 
DROP COLUMN nr_latitude;

ALTER TABLE datawarehouse.dim_bairro_comparado 
DROP COLUMN nr_longitude;

ALTER TABLE datawarehouse.dim_bairro_comparado 
DROP COLUMN bairro_normalizado;

ALTER TABLE datawarehouse.dim_bairro
ALTER COLUMN fk_id_sg_uf
TYPE INTEGER USING fk_id_sg_uf::INTEGER;

create table datawarehouse.fato_eleicoes
(id serial primary key,
fk_id_ano integer REFERENCES datawarehouse.dim_ano(id) ,
fk_id_bairro integer REFERENCES datawarehouse.dim_bairro(id) ,
fk_id_cep integer REFERENCES datawarehouse.dim_cep_lt_lg(id),
fk_data_eleicao integer REFERENCES datawarehouse.dim_data_eleicao(id),
fk_ds_eleicao integer REFERENCES datawarehouse.dim_ds_eleicao(id),
fk_id_legenda integer REFERENCES datawarehouse.dim_legenda(id),
fk_id_municipio integer REFERENCES datawarehouse.dim_municipio(id) ,
fk_ik_nome_candidato integer REFERENCES datawarehouse.dim_nome_candidato(id),
fk_id_turno integer REFERENCES datawarehouse.dim_turno(id),
fk_id_zona integer REFERENCES datawarehouse.dim_zonas_eleitorais(id),
qtd_votos integer,
qtd_abstencoes integer,
qtd_aptos integer
)

select distinct
da.id ,
dde.id,
dl.id ,
dm.id ,
dsu.id,
dze.id,
from public.eleicoes pe
inner join  datawarehouse.dim_ano as da on da.ano = pe.aa_eleicao
inner join datawarehouse.dim_data_eleicao as dde on dde.fk_id_ano = da.id
inner join datawarehouse.dim_legenda as dl on dl.nr_votavel = pe.nr_votavel
inner join datawarehouse.dim_municipio as dm on dm.cd_municipio = pe.cd_municipio
inner join datawarehouse.dim_sg_uf as dsu on dsu.sg_uf = dsu.sg_uf
inner join datawarehouse.dim_zonas_eleitorais as dze on dze.nr_zona = pe.nr_zona

dt.id 
inner join datawarehouse.dim_turno as dt on dt.nr_turno = pe.nr_turno
dnc.id,
inner join datawarehouse.dim_nome_candidato as dnc on dnc.nm_votavel = pe.nm_votavel
de.id
inner join datawarehouse.dim_ds_eleicao as de on de.ds_eleicao = pe.ds_eleicao




dib.id,
dcep.id,
pe.qt_votos,
pe.qt_abstencoes,
pe.qt_aptos



inner join datawarehouse.dim_bairro as dib on dib.fk_id_municipio = dm.id 
inner join datawarehouse.dim_cep_lt_lg as dcep on dcep.id = dib.fk_id_cep

 








30_11_2024
select distinct
da.id,
dbr.id,
dcep.id,
dt.id,
dl.id,
dze.id,
pe.qt_votos,
pe.qt_abstencoes,
pe.qt_aptos
from datawarehouse.dim_bairro dbr
inner join datawarehouse.dim_ano as da on da.id = dbr.fk_id_ano
inner join datawarehouse.dim_cep_lt_lg as dcep on dcep.id = dbr.fk_id_cep
inner join public.eleicoes as pe on pe.aa_eleicao = da.ano
inner join datawarehouse.dim_turno as dt on dt.nr_turno = pe.nr_turno
inner join datawarehouse.dim_legenda as dl on dl.nr_votavel = pe.nr_votavel
inner join datawarehouse.dim_zonas_eleitorais as dze on dze.fk_id_municipio = dbr.fk_id_municipio


create table datawarehouse.fato_eleicoes_02
(id serial PRIMARY key,
ano_eleicao integer,
uf char (2),
cd_municipio integer,
nr_zona integer,
nr_votavel bigint,
qt_votos integer,
qt_abstencoes integer,
qt_aptos integer)


insert into datawarehouse.fato_eleicoes_02 (ano_eleicao,
uf, cd_municipio, nr_zona, nr_votavel, qt_votos ,qt_abstencoes, qt_aptos)
select DISTINCT 
aa_eleicao,
sg_uf,
cd_municipio,
nr_zona,
nr_votavel,
qt_votos,
qt_abstencoes,
qt_aptos
from public.eleicoes

alter table datawarehouse.fato_eleicoes_02
add column fk_id_cep integer

update datawarehouse.fato_eleicoes_02 fe
set uf = dsu.id
from datawarehouse.dim_sg_uf dsu
where dsu.sg_uf = fe.uf

ALTER TABLE datawarehouse.fato_eleicoes_02
ALTER COLUMN fk_id_uf TYPE integer
USING fk_id_uf::integer;

update datawarehouse.fato_eleicoes_02 fe
set fk_id_zona = dze.id
from datawarehouse.dim_zonas_eleitorais dze
where dze.nr_zona = fe.fk_id_zona

update datawarehouse.fato_eleicoes_02 fe
set fk_id_municipio = dm.id
from datawarehouse.dim_municipio dm
where fe.fk_id_municipio = dm.cd_municipio

update datawarehouse.fato_eleicoes_02 fe
set fk_dim_legenda = dl.id
from datawarehouse.dim_legenda dl
where fe.fk_dim_legenda = dl.nr_votavel

--preenchar a fato e a dim_zona
datawarehouse.fato_eleicoes_02
id
fk_dim_ano
fk_id_uf
fk_id_municipio
fk_id_zona
fk_id_cep (esta vazia precisa popular com a chave de cep)
datawarehouse.dim_zonas_eleitorais
id
fk_id_uf
fk_id_municipio
nr_zona
fk_id_bairro((esta vazia precisa popular com a chave de bairro)
fk_id_ano((esta vazia precisa popular com a chave de ano)
datawarehouse.dim_bairro
fk_id_ano
fk_id_sg_uf
fk_id_municipio
fk_id_cep
datawarehouse.dim_ano
id
ano
datawarehouse.dim_cep_lt_lg
id
cep
latitude
longitude
datawarehouse.dim_data_eleicao
id
dia
fk_id_ano
fk_id_mes
datawarehouse.dim_legenda
id
nr_votavel
datawarehouse.dim_municipio
id
cd_municipio
nm_municipio
fk_id_sg_uf
datawarehouse.dim_nome_candidato
id
nm_votavel
legenda
fk_id_dim_legenda
datawarehouse.dim_sg_uf
id
sg_uf
datawarehouse.dim_turno
id
nr_turno

UPDATE datawarehouse.fato_eleicoes_02 fato
SET fk_id_cep = (
    SELECT cep.id
    FROM datawarehouse.dim_cep_lt_lg cep
    JOIN datawarehouse.dim_municipio mun ON fato.fk_id_municipio = mun.id
    WHERE cep.cep = (
        SELECT dim_cep_lt_lg.cep
        FROM datawarehouse.dim_cep_lt_lg
        WHERE dim_cep_lt_lg.id = fato.fk_id_cep
    )
);

UPDATE datawarehouse.dim_zonas_eleitorais zona
SET fk_id_bairro = (
    SELECT bairro.id
    FROM datawarehouse.dim_bairro bairro
    WHERE bairro.fk_id_municipio = zona.fk_id_municipio
    AND bairro.fk_id_sg_uf = zona.fk_id_uf
);

UPDATE datawarehouse.dim_zonas_eleitorais zona
SET fk_id_ano = (
    SELECT ano.id
    FROM datawarehouse.dim_ano ano
    WHERE ano.ano = EXTRACT(YEAR FROM CURRENT_DATE) -- Substitua pelo ano desejado
);

02/12/2024

--ALTERAÇÕES NA FATO ELEIÇÃO
--sg_uf vira fk_sg_uf
UPDATE datawarehouse.fato_eleicao fe
SET sg_uf = dsu.id
FROM datawarehouse.dim_sg_uf dsu
WHERE dsu.sg_uf = fe.sg_uf

--alterar de char para integer
ALTER TABLE datawarehouse.fato_eleicao
ALTER COLUMN sg_uf TYPE integer
USING CAST(sg_uf AS integer);

--aa_eleicao vira fk_dim_ano
UPDATE datawarehouse.fato_eleicao fe
SET aa_eleicao = da.id
FROM datawarehouse.dim_ano da
WHERE da.ano = fe.aa_eleicao

--cd_municipio vira fk_dim_municipio
UPDATE datawarehouse.fato_eleicao fe
SET cd_municipio = dm.id
FROM datawarehouse.dim_municipio dm
WHERE dm.cd_municipio = fe.cd_municipio

--nr_zona vira fk_dim_zona
UPDATE datawarehouse.fato_eleicao fe
SET nr_zona = dze.id
FROM datawarehouse.dim_zonas_eleitorais dze
WHERE dze.nr_zona = fe.nr_zona

--falta a partir daqui !!!!!!!

--nr_turno vira fk_dim_turno
UPDATE datawarehouse.fato_eleicao fe
SET nr_turno = dt.id
FROM datawarehouse.dim_turno dt
WHERE dt.nr_turno = fe.nr_turno

--nr_votavel vira fk_dim_legenda
UPDATE datawarehouse.fato_eleicao fe
SET nr_votavel = dl.id
FROM datawarehouse.dim_legenda dl
WHERE dl.nr_votavel = fe.nr_votavel

select distinct count(fk_dim_id_cep) from datawarehouse.fato_cep_python

select distinct count(id) from datawarehouse.dim_cep_lt_lg

select distinct count(fk_id_cep) from datawarehouse.dim_bairro

04_12_2024
update datawarehouse.fato_cep fcep
set nr_cep = dcep.id
from datawarehouse.dim_cep dcep
where fcep.nr_cep = dcep.nr_cep

select bairro_normalizado from datawarehouse.dim_bairro
where bairro_normalizado is NULL

select bairro_normalizado from datawarehouse.fato_cep
where bairro_normalizado is NULL

select nr_cep from datawarehouse.fato_cep
where nr_cep is NULL

select DISTINCT (cd_municipio) from datawarehouse.dim_municipio
select DISTINCT (cd_municipio) from datawarehouse.fato_cep

select DISTINCT (nr_cep) from datawarehouse.dim_cep
select DISTINCT (nr_cep) from datawarehouse.fato_cep

select distinct 
nr_zona,
cd_municipio,
nm_municipio
from datawarehouse.fato_eleicao fe
inner join datawarehouse.fato_cep as fcep on fcep.nr_zona 

CREATE TABLE IF NOT EXISTS datawaredim_zona (
    id SERIAL PRIMARY KEY,
    sg_uf CHAR(2),
    cd_municipio INTEGER,
    nr_zona BIGINT,
    UNIQUE (sg_uf, cd_municipio, nr_zona)
);

-- Etapa 2: Inserir dados únicos das tabelas fato_cep e fato_eleicao
INSERT INTO datawarehouse.dim_zona (sg_uf, cd_municipio, nr_zona)
SELECT DISTINCT sg_uf, cd_municipio, nr_zona
FROM (
    SELECT sg_uf, cd_municipio, nr_zona FROM datawarehouse.fato_cep
    UNION
    SELECT sg_uf, cd_municipio, nr_zona FROM datawarehouse.fato_eleicao
) AS zonas_unificadas;

adiconar em dim_municipio

INSERT INTO datawarehouse.dim_municipio (sg_uf, cd_municipio,nm_municipio )
SELECT DISTINCT sg_uf, cd_municipio, nm_municipio
FROM (
    SELECT sg_uf, cd_municipio, nm_municipio FROM datawarehouse.fato_cep
    UNION
    SELECT sg_uf, cd_municipio FROM datawarehouse.fato_eleicao
) AS zonas_unificadas;




update datawarehouse.dim_zona dz
set nr_cep = fcep.nr_cep 
from datawarehouse.fato_cep fcep
where dz.sg_uf = fcep.sg_uf
and dz.cd_municipio = fcep.cd_municipio
and dz.nr_zona = fcep.nr_zona

--verifica os nulos e estão aparecendo os do ano de 2008
-- existem 136 zonas que não achei o cep
select * from datawarehouse.dim_zona
where nr_cep is null
order by cd_municipio

select * from datawarehouse.fato_cep
where nr_cep ilike '0'

alter table datawarehouse.dim_zona
add column nm_bairro text

update datawarehouse.dim_zona dz
set nm_bairro = fe.nm_bairro
from datawarehouse.fato_cep fe
where fe.nr_cep = dz.nr_cep

update datawarehouse.dim_zona dz
set nm_bairro = fe.nm_bairro
from datawarehouse.fato_cep fe
where fe.nr_cep = dz.nr_cep

select nm_bairro,nr_cep from datawarehouse.fato_cep
where nr_zona = 340
and cd_municipio = 46132

SELECT nr_zona,cd_municipio,sg_uf,aa_eleicao from datawarehouse.fato_eleicao
where nr_zona = 340
and cd_municipio = 46132

select * from datawarehouse.dim_zona

SELECT cd_municipio from datawarehouse.dim_municipio


--colocando as chaves estrangeiras na fato_cep
update datawarehouse.dim_zona dz
set sg_uf = dsu.id
from datawarehouse.dim_sg_uf dsu
where dz.sg_uf = dsu.sg_uf

ALTER TABLE datawarehouse.dim_zona
ALTER COLUMN fk_dim_sg_uf TYPE integer
USING fk_dim_sg_uf::integer;

update datawarehouse.dim_zona dz
set fk_dim_sg_uf = dm.id
from datawarehouse.dim_municipio dm
where dz.fk_dim_sg_uf = dm.cd_municipio

12818 1074	"CRUZEIRO DO SUL"	"AC"
select id, cd_municipio, nm_municipio, sg_uf from datawarehouse.dim_municipio 
order by sg_uf

select fk_dim_sg_uf fk_dim_municipio from datawarehouse.dim_municipio 


update datawarehouse.dim_zona dz
set nr_cep = dcep.id
from datawarehouse.dim_cep dcep
where dz.nr_cep = dcep.nr_cep

update datawarehouse.dim_zona dz
set fk_dim_cep = '0'
where fk_dim_cep = '0.0'

update datawarehouse.dim_zona dz
set fk_dim_cep = '88587'
where fk_dim_cep = '88587.0'

ALTER TABLE datawarehouse.dim_zona
ALTER COLUMN fk_dim_cep TYPE integer
USING fk_dim_cep::integer;

update datawarehouse.dim_zona dz
set bairro_normalizado = dbr.id
from datawarehouse.dim_bairro dbr
where dz.bairro_normalizado = dbr.bairro_normalizado

alter table datawarehouse.dim_zona
add column bairro_normalizado text

update datawarehouse.dim_zona dz
set bairro_normalizado = fcep.bairro_normalizado
from datawarehouse.fato_cep fcep
where fcep.nm_bairro = dz.nm_bairro

ALTER TABLE datawarehouse.dim_zona
ALTER COLUMN bairro_normalizado TYPE integer
USING bairro_normalizado::integer;

update datawarehouse.fato_cep fcep
set bairro_normalizado = dbr.id
from datawarehouse.dim_bairro dbr
where dbr.bairro_normalizado = fcep.bairro_normalizado

ALTER TABLE datawarehouse.fato_cep
ALTER COLUMN fk_dim_bairro TYPE integer
USING fk_dim_bairro::integer;

update datawarehouse.fato_cep fcep
set nr_cep = dcep.id
from datawarehouse.dim_cep dcep
where dcep.nr_cep = fcep.nr_cep

ALTER TABLE datawarehouse.fato_cep
ALTER COLUMN  nr_cep TYPE integer
USING nr_cep::integer;

update datawarehouse.fato_cep fcep
set nr_zona = dz.id
from datawarehouse.dim_zona dz
where dz.nr_zona = fcep.nr_zona

INSERT INTO datawarehouse.dim_municipio (cd_municipio, nm_municipio, sg_uf)
SELECT DISTINCT fcep.cd_municipio, fcep.nm_municipio, fcep.sg_uf
FROM datawarehouse.fato_cep fcep;


select distinct cd_municipio,nm_municipio from datawarehouse.fato_cep
order by cd_municipio
--5616 resultados

select distinct cd_municipio,nm_municipio from public.eleicoes
order by cd_municipio
--5602 

select distinct cd_municipio,nm_municipio from public.tabela_cep_2012
order by cd_municipio
--5568

select distinct cd_municipio,nm_municipio from public.tabela_cep_2016
order by cd_municipio
--5568

select distinct cd_municipio,nm_municipio from public.tabela_cep_2020
order by cd_municipio
--5568

update datawarehouse.fato_cep fcep
set cd_municipio = dm.id
from datawarehouse.dim_municipio dm
where dm.cd_municipio = fcep.cd_municipio

select 
dm.nm_municipio,
fcep.fk_dim_municipio
from datawarehouse.fato_cep fcep
inner join datawarehouse.dim_municipio as dm on dm.id = fcep.fk_dim_municipio

select id, nm_municipio from datawarehouse.dim_municipio
where  nm_municipio ilike 'teresina'

update datawarehouse.fato_cep fcep
set sg_uf = dsg.id
from datawarehouse.dim_sg_uf dsg
where dsg.sg_uf = fcep.sg_uf

ALTER TABLE datawarehouse.fato_cep
ALTER COLUMN fk_dim_sg_uf  TYPE integer
USING fk_dim_sg_uf::integer;


--colocando as chaves estrangeiras na fato_eleicao

update datawarehouse.fato_eleicao fe
set aa_eleicao = da.id
from datawarehouse.dim_ano da
where da.ano = fe.aa_eleicao

update datawarehouse.fato_eleicao fe
set sg_uf = dsg.id
from datawarehouse.dim_sg_uf dsg
where dsg.sg_uf = fe.sg_uf

ALTER TABLE datawarehouse.fato_eleicao
ALTER COLUMN  sg_uf TYPE integer
USING sg_uf ::integer;

update datawarehouse.fato_eleicao fe
set cd_municipio = dm.id
from datawarehouse.dim_municipio dm
where dm.cd_municipio = fe.cd_municipio

update datawarehouse.fato_eleicao fe
set nr_zona = dz.id
from datawarehouse.dim_zona dz
where dz.nr_zona = fe.nr_zona

update datawarehouse.fato_eleicao fe
set nr_turno = dt.id
from datawarehouse.dim_turno dt
where dt.nr_turno = fe.nr_turno

update datawarehouse.fato_eleicao fe
set nr_votavel = dl.id
from datawarehouse.dim_legenda dl
where dl.nr_votavel = fe.nr_votavel

05/12/2024

SELECT 
    nr_latitude AS latitude,
    nr_longitude AS longitude
FROM 
    datawarehouse.fato_cep
	limit 30


UPDATE sua_tabela
SET latitude = NULL, longitude = NULL
WHERE latitude = -1.0 AND longitude = -1.0;

ALTER TABLE datawarehouse.fato_cep
ALTER COLUMN nr_latitude TYPE NUMERIC(10,6),
ALTER COLUMN nr_longitude TYPE NUMERIC(10,6);

ALTER TABLE datawarehouse.dim_cep
ALTER COLUMN nr_latitude TYPE NUMERIC(10,6),
ALTER COLUMN nr_longitude TYPE NUMERIC(10,6);


ALTER TABLE datawarehouse.fato_cep
ALTER COLUMN nr_latitude TYPE NUMERIC(10,6),
ALTER COLUMN nr_longitude TYPE NUMERIC(10,6);

ALTER TABLE datawarehouse.dim_cep
ALTER COLUMN nr_latitude TYPE NUMERIC(10,6),
ALTER COLUMN nr_longitude TYPE NUMERIC(10,6);

select distinct
da.ano,
dsu.sg_uf,
dz.nr_zona,
dl.nr_votavel,
dbr.bairro_normalizado,
fe.qt_votos
from datawarehouse.fato_eleicao fe 
inner join datawarehouse.dim_ano as da on da.id = fe.fk_dim_ano
inner join datawarehouse.fato_cep as fcep on fcep.fk_dim_zona = fe.fk_dim_zona
inner join datawarehouse.dim_sg_uf as dsu on dsu.id = fe.fk_dim_sg_uf
inner join datawarehouse.dim_bairro as dbr on dbr.id = fcep.fk_dim_bairro
INNER join datawarehouse.dim_zona as dz on dz.fk_dim_sg_uf = dsu.id
inner join datawarehouse.dim_legenda as dl on dl.id = fe.fk_nr_votavel

select distinct sum(qt_votos) from public.eleicoes
WHERE sg_uf = 'CE'
and aa_eleicao = '2012'


select distinct
dsu.sg_uf,
da.ano,
sum(qt_votos) as soma
from datawarehouse.fato_eleicao fe
inner join datawarehouse.dim_sg_uf as dsu on dsu.id = fe.fk_dim_sg_uf
inner join datawarehouse.dim_ano as da on da.id = fe.fk_dim_ano
WHERE dsu.sg_uf = 'CE'
and da.ano = '2012'
group by dsu.sg_uf,da.ano;






o quanto os partidos de direita / esquerda creceram ao longo das ultimas 4 eleições 
	-delimitar por por municipio
	-delimitar por estado
	- delimitar por região

(GABRIEL) 2 historico de 1 e 2 turnos 
	- quais foram os partidos que se elegeram em primeiro turno.(partidos fortes)

(JONATHAN)3  Quanto o fundo eleitoral beneficiou os partidos
	- vincular a tabela externa (achar a tabela de valores)
	- quanto cada candidato eleito gastou em media
		- delimitar por ano
		- delimitar por partido

(JOSIANE) 4 aumento / diminuição de votos brancos e nulos 
	-  delimitar por por municipio
	-  delimitar por estado
	-  delimitar por região

07/12/2024


CREATE TABLE datawarehouse.cep_denovo(
	id serial primary key,
    nr_cep integer,
    nr_latitude numeric,
    nr_longitude numeric,
	nr_zona integer,
	ds_endereco CHARACTER VARYING(100),
	cd_municipio BIGINT,
	sg_uf CHARACTER VARYING (30),
    aa_eleicao BIGINT,
	nm_bairro CHARACTER VARYING (100),
    nm_municipio CHARACTER VARYING (100),
    fk_id_ano INTEGER
);



INSERT INTO datawarehouse.cep_denovo (nr_cep, nr_latitude, nr_longitude,ds_endereco, nr_zona,cd_municipio,sg_uf,
    aa_eleicao, nm_bairro, nm_municipio, fk_id_ano)
SELECT DISTINCT
        t.nr_cep,
        t.nr_latitude,
        t.nr_longitude,
		unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.ds_endereco, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
		t.nr_zona,  -- Casting explícito para INTEGER
        t.cd_municipio,  -- Casting explícito para INTEGER
        t.sg_uf,
        t.aa_eleicao,
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_bairro, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_municipio, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
		da.id AS fk_id_ano
FROM public.tabela_cep_2012 t
INNER JOIN datawarehouse.dim_ano da ON da.ano = t.aa_eleicao
UNION ALL
SELECT DISTINCT
        t.nr_cep,
        t.nr_latitude,
        t.nr_longitude,
		unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.ds_endereco, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
		t.nr_zona,  -- Casting explícito para INTEGER
        t.cd_municipio,  -- Casting explícito para INTEGER
        t.sg_uf,
        t.aa_eleicao,
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_bairro, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_municipio, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
		da.id AS fk_id_ano
FROM public.tabela_cep_2016 t
INNER JOIN datawarehouse.dim_ano da ON da.ano = t.aa_eleicao
UNION ALL
SELECT DISTINCT
        t.nr_cep,
        t.nr_latitude,
        t.nr_longitude,
		unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.ds_endereco, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
		t.nr_zona,  -- Casting explícito para INTEGER
        t.cd_municipio,  -- Casting explícito para INTEGER
        t.sg_uf,
        t.aa_eleicao,
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_bairro, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_municipio, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
		da.id AS fk_id_ano
FROM public.tabela_cep_2020 t
INNER JOIN datawarehouse.dim_ano da ON da.ano = t.aa_eleicao


-- create table datawarehouse.dim_bairro
-- (id serial primary key,
-- nm_bairro TEXT,
-- fk_id_cd_municipio bigint,
-- fk_id_sg_uf CHARACTER VARYING (2),
-- fk_id_cep integer)

-- insert into datawarehouse.dim_bairro(nm_bairro, fk_id_cd_municipio,fk_id_sg_uf,fk_id_cep)
-- select distinct
-- nm_bairro,
-- cd_municipio,
-- sg_uf,
-- nr_cep 
-- from datawarehouse.cep_denovo

-- ALTER TABLE public.tabela_cep_2012
-- ALTER COLUMN nm_municipio TYPE CHARACTER VARYING(100);

-- update datawarehouse.dim_bairro
-- set fk_id_sg_uf = dsu.id
-- from datawarehouse.dim_sg_uf dsu
-- where dsu.sg_uf = fk_id_sg_uf

-- CREATE TABLE datawarehouse.dim_cep(
-- 	id serial primary key,
--     nr_cep integer,
--     nr_latitude numeric,
--     nr_longitude numeric );

-- insert into datawarehouse.dim_cep(nr_cep,nr_latitude,nr_longitude)
-- select distinct
-- nr_cep,
-- nr_latitude,
-- nr_longitude
-- from datawarehouse.cep_denovo

update datawarehouse.dim_bairro
set fk_id_cep = dcep.id
from datawarehouse.dim_cep dcep
where nr_cep = fk_id_cep

CREATE TABLE datawarehouse.dim_municipio(
	id serial primary key,
	cd_municipio bigint,
	sg_uf CHARACTER VARYING (2),
    nm_municipio CHARACTER VARYING (100)
);
insert into datawarehouse.dim_municipio(cd_municipio, sg_uf, nm_municipio)
select distinct
cd_municipio,
sg_uf,
nm_municipio 
from datawarehouse.cep_denovo

update datawarehouse.dim_bairro
set fk_id_cd_municipio = dm.id
from datawarehouse.dim_municipio dm
where dm.cd_municipio = fk_id_cd_municipio

update datawarehouse.dim_municipio dm
set sg_uf = dsu.id
from  datawarehouse.dim_sg_uf dsu
where dsu.sg_uf = dm.sg_uf

ALTER TABLE datawarehouse.dim_municipio
ALTER COLUMN fk_id_sg_uf TYPE integer USING fk_id_sg_uf::integer;

ALTER TABLE datawarehouse.dim_bairro
ALTER COLUMN fk_id_sg_uf TYPE integer USING fk_id_sg_uf::integer;

insert into datawarehouse.dim_zona(fk_dim_sg_uf, fk_dim_bairro, fk_dim_municipio, nr_zona,fk_dim_cep)
select distinct
dsu.id as fk_dim_sg_uf,
dbr.id as fk_dim_bairro,
dm.id as fk_dim_municipio,
fcepnovo.nr_zona as nr_zona,
dcep.id as fk_dim_cep
from datawarehouse.cep_denovo fcepnovo
inner join datawarehouse.dim_sg_uf as dsu on dsu.sg_uf = fcepnovo.sg_uf
inner join datawarehouse.dim_municipio as dm on dm.nm_municipio = fcepnovo.nm_municipio
inner join datawarehouse.dim_cep as dcep on dcep.nr_cep = fcepnovo.nr_cep
inner join datawarehouse.dim_bairro as dbr on dbr.nm_bairro = fcepnovo.nm_bairro

–faltam as tabelas fato, 



09/12/2024
hoje:
SELECT * FROM public.eleicoes
LIMIT 100

SELECT * FROM datawarehouse.fato_eleicao
ORDER BY id ASC LIMIT 100

SELECT distinct
dcep.nr_cep,
dcep.nr_latitude,
dcep.nr_longitude,
dbr.nm_bairro,
dm.nm_municipio,
dsu.sg_uf
FROM datawarehouse.dim_cep dcep
inner join datawarehouse.dim_bairro as dbr on dbr.fk_id_cep = dcep.id
inner join datawarehouse.dim_municipio as dm on dm.id = dbr.fk_id_cd_municipio
inner join datawarehouse.dim_sg_uf as dsu on dsu.id = dm.fk_id_sg_uf
WHERE LENGTH(nr_cep::TEXT) < 8
order by nr_latitude

SELECT distinct nr_cep, nr_latitude,nr_longitude
FROM datawarehouse.dim_cep
WHERE LENGTH(nr_cep::TEXT) < 8
order by nr_latitude

SELECT nr_cep
FROM datawarehouse.dim_cep
WHERE LENGTH(nr_cep::TEXT) < 8;

SELECT distinct (LENGTH(nr_cep::TEXT)<8) AS tamanho
FROM datawarehouse.dim_cep;



alter table datawarehouse.fato_eleicao 
ALTER COLUMN fk_dim_sg_uf TYPE integer USING fk_dim_sg_uf::integer;

update datawarehouse.fato_eleicao
set fk_nr_votavel = dl.id
from  datawarehouse.dim_legenda dl
where  dl.nr_votavel = fk_nr_votavel

update datawarehouse.fato_eleicao
set fk_dim_turno = dt.id
from  datawarehouse.dim_turno dt
where  dt.nr_turno = fk_dim_turno

update datawarehouse.fato_eleicao
set fk_dim_zona = dz.id
from datawarehouse.dim_zona dz
where  dz.nr_zona = fk_dim_zona

update datawarehouse.fato_eleicao
set   fk_dim_sg_uf = dsu.id
from datawarehouse.dim_sg_uf dsu
where  dsu.sg_uf = fk_dim_sg_uf
update datawarehouse.fato_eleicao
set   fk_dim_cd_municipio = dm.id
from datawarehouse.dim_municipio dm
where  dm.fk_id_cd_municipio = fk_dim_cd_municipio


select distinct
ds_endereco,
nm_municipio,
sg_uf,
from public.tabela_cep_2012
WHERE LENGTH(nr_cep::TEXT) < 8

10_12_2024

-- INSERT INTO datawarehouse.endereço_cep(ds_endereco, nm_municipio, sg_uf, nr_cep)
--   SELECT DISTINCT
--             unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(ds_endereco, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))) AS ds_endereco,
--             unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(nm_municipio, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))) AS nm_municipio,
--             sg_uf,
--             nr_cep
--         FROM public.tabela_cep_2012
     
--         UNION
--         SELECT DISTINCT
--             unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(ds_endereco, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))) AS ds_endereco,
--             unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(nm_municipio, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))) AS nm_municipio,
--             sg_uf,
--             nr_cep
--         FROM public.tabela_cep_2016
        
--         UNION
--         SELECT DISTINCT
--             unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(ds_endereco, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))) AS ds_endereco,
--             unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(nm_municipio, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))) AS nm_municipio,
--             sg_uf,
--             nr_cep
--         FROM public.tabela_cep_2020

--   CREATE TABLE IF NOT EXISTS datawarehouse.endereço_cep(
--         id SERIAL PRIMARY KEY,
--         ds_endereco CHARACTER VARYING(100),
--         nm_municipio CHARACTER VARYING(100),
--         sg_uf CHARACTER VARYING(2),
--         nr_cep INTEGER
--     );

-- update datawarehouse.endereço_cep ec
-- set nm_municipio = dm.id
-- from datawarehouse.dim_municipio dm
-- where dm.nm_municipio =  ec.nm_municipio

-- update datawarehouse.endereço_cep ec
-- set sg_uf = dsu.id
-- from datawarehouse.dim_sg_uf dsu
-- where dsu.sg_uf =  ec.sg_uf

-- update datawarehouse.endereço_cep ec
-- set nr_cep = dc.id
-- from datawarehouse.dim_cep dc
-- where dc.nr_cep =  ec.nr_cep

-- select  
-- de.ds_endereco,
-- dc.nr_cep,
-- dsu.sg_uf,
-- dm.nm_municipio
-- from datawarehouse.dim_endereco de
-- inner join datawarehouse.dim_cep as dc on dc.id = de.fk_nr_cep
-- inner join datawarehouse.dim_sg_uf as dsu on dsu.id = de.fk_sg_uf
-- inner join datawarehouse.dim_municipio as dm on dm.id = de.fk_nm_municipio
-- WHERE LENGTH(nr_cep::TEXT) < 8


-- alter table datawarehouse."dim_endereço"
-- ALTER COLUMN fk_nm_municipio TYPE integer USING fk_nm_municipio::integer;

-- alter table datawarehouse."dim_endereço"
-- ALTER COLUMN fk_sg_uf TYPE integer USING fk_sg_uf::integer;

-- alter table datawarehouse."dim_endereço"
-- ALTER COLUMN fk_nr_cep TYPE integer USING fk_nr_cep::integer;

select 
ds_endereco,
dm.nm_municipio,
dsu.sg_uf,
dcep.nr_cep
from datawarehouse.dim_endereco as de
inner join datawarehouse.dim_sg_uf as dsu ON dsu.id = de.fk_sg_uf
inner join datawarehouse.dim_cep as dcep on dcep.id = de.fk_nr_cep
inner join datawarehouse.dim_municipio as dm on dm.id = de.fk_nm_municipio
update datawarehouse.cep_denovo
where length(nr_cep::text) < 8

update datawarehouse.dim_endereco
set fk_nr_cep = dcep.nr_cep
from datawarehouse.dim_cep dcep
where fk_nr_cep = dcep.id

select distinct * from datawarehouse.cep_denovo
where length(nr_cep::text) < 8

select distinct * from datawarehouse.cepes_api
where length(nr_cep::text) < 8

UPDATE datawarehouse.cep_denovo cp
SET nr_cep = api.cep_corrigido
FROM datawarehouse.cepes_api api
WHERE LENGTH(cp.nr_cep::text) < 8
  AND cp.ds_endereco = api.ds_endereco
  AND cp.sg_uf = api.sg_uf
  AND cp.nm_municipio = api.nm_municipio;

UPDATE datawarehouse.cepes_api
SET cep_corrigido = REGEXP_REPLACE(cep_corrigido, '-', '', 'g')
WHERE cep_corrigido LIKE '%-%';

alter table datawarehouse.cepes_api
alter COLUMN cep_corrigido TYPE integer USING cep_corrigido::integer


update datawarehouse.cepes_api
set cep_corrigido = 0
where cep_corrigido is null

UPDATE datawarehouse.cepes_api
SET cep_corrigido = REPLACE(cep_corrigido, 'k', '')
WHERE cep_corrigido LIKE '%k%';


select distinct * from datawarehouse.cep_denovo
where nr_cep like '00000000'

UPDATE datawarehouse.cep_denovo
SET nr_cep = CONCAT('0000000', nr_cep)
WHERE LENGTH(nr_cep::text) = 1;

alter table datawarehouse.cep_denovo
alter COLUMN nr_cep TYPE varchar USING nr_cep::varchar

select * from datawarehouse.cepes_api_02
order by cep_corrigido

UPDATE datawarehouse.cepes_api_02
SET cep_corrigido = REGEXP_REPLACE(cep_corrigido, '-', '', 'g')
WHERE cep_corrigido LIKE '%-%';

UPDATE datawarehouse.cepes_api_02
SET cep_corrigido = CONCAT( nr_cep,'00000000')
WHERE LENGTH(cep_corrigido::text) is null ;

UPDATE datawarehouse.cep_denovo cp
SET nr_cep = api2.cep_corrigido
FROM datawarehouse.cepes_api_02 api2
 where cp.ds_endereco = api2.ds_endereco
  AND cp.sg_uf = api2.sg_uf
  AND cp.nm_municipio = api2.nm_municipio
  and cep_corrigido <> '00000000';


select distinct * from datawarehouse.cep_denovo
where nr_cep = '000000000'
where nr_cep is null

update datawarehouse.dim_cep dcep
set nr_cep = dcpnovo.nr_cep 
from datawarehouse.cep_denovo dcpnovo
where dcep.nr_latitude = dcpnovo.nr_latitude
and dcep.nr_longitude = dcpnovo.nr_longitude

select distinct * from datawarehouse.cep_denovo
where nr_cep = '07243100'

update datawarehouse.dim_cep
set nr_cep = 07243100
where nr_cep = 7243100

select distinct * from datawarehouse.dim_cep
where nr_cep = '000000000'

update datawarehouse.dim_cep
set nr_cep = '60000000'
where nr_cep = '000000000'

alter table datawarehouse.dim_cep
alter column nr_cep TYPE varchar USING nr_cep::varchar

select distinct * from datawarehouse.dim_cep
where nr_cep = '60000000'

update  datawarehouse.cep_denovo
set nr_cep = '60000000'
where nr_cep = '000000000'

select distinct * from datawarehouse.cepes_api_04
order by cep_corrigido

UPDATE datawarehouse.cep_denovo cp
SET nr_cep = api4.cep_corrigido
FROM datawarehouse.cepes_api_04 api4
 where cp.ds_endereco = api4.ds_endereco
  AND cp.sg_uf = api4.sg_uf
  AND cp.nm_municipio = api4.nm_municipio
  and cep_corrigido <> '00000000';

update datawarehouse.dim_cep dcep
set nr_cep = dcpnovo.nr_cep 
from datawarehouse.cep_denovo dcpnovo
where dcep.nr_latitude = dcpnovo.nr_latitude
and dcep.nr_longitude = dcpnovo.nr_longitude

select distinct * from datawarehouse.dim_cep
where nr_cep = '60000000'

UPDATE datawarehouse.fato_cep
SET fk_dim_cep = NULL;

UPDATE datawarehouse.dim_zona
SET fk_dim_cep = NULL;

UPDATE datawarehouse.dim_endereco
SET fk_nr_cep = NULL;

UPDATE datawarehouse.dim_bairro
SET fk_id_cep = NULL;

create table datawarehouse.dim_cep
(id serial primary key,
nr_cep varchar(8),
nr_latitude numeric,
nr_longitude numeric);

select nr_cep from datawarehouse.cep_denovo
where length(nr_cep)<8

update datawarehouse.cep_denovo
SET nr_cep = REGEXP_REPLACE(nr_cep, '-', '', 'g')

update datawarehouse.cep_denovo
SET nr_cep = concat(nr_cep, '000')

UPDATE datawarehouse.cep_denovo
SET nr_cep = LEFT(nr_cep, LENGTH(nr_cep) - 3)
where length(nr_cep)>8

insert into datawarehouse.dim_cep
(nr_cep,nr_latitude,nr_longitude)
select distinct nr_cep,nr_latitude,nr_longitude
from datawarehouse.cep_denovo

update datawarehouse.dim_bairro
set fk_id_cep = datawarehouse.dim_cep.id
from datawarehouse.dim_cep
where 
datawarehouse.dim_bairro.fk_id_cd_municipio = datawarehouse.dim_municipio.id
datawarehouse.dim_bairro.fk_id_sg_uf = datawarehouse.dim_sg_uf.id

update datawarehouse.dim_bairro dbr
set fk_id_cep = nr_cep
from datawarehouse.cep_denovo cdn 
where dbr.nm_bairro = cdn.nm_bairro
and dbr.fk_id_sg_uf = cdn.sg_uf
and dbr.fk_id_cd_municipio = (select cdn.cd_municipio from 

select * from datawarehouse.cep_denovo 
where cd_municipio = 3209
and nm_municipio = 'CAMPESTRE DA SERRA'

update datawarehouse.dim_endereco

11_12_2024

select * from datawarehouse.dim_endereco

-- update datawarehouse.cep_denovo
-- set  nr_cep  = 68880000
-- where nr_cep like '000000%'

-- update datawarehouse.cep_denovo cdn
-- set nr_cep = api_04.cep_corrigido
-- from datawarehouse.cepes_api_04 api_04
-- where cdn.sg_uf = 'SP'
-- and api_04.ds_endereco = cdn.ds_endereco
-- and cdn.nm_municipio = api_04.nm_municipio

-- SELECT distinct * FROM datawarehouse.cep_denovo
-- where length (nr_cep)=1 -- aqui foi testado 1,2,3,4,5,6,7
-- order by sg_uf

-- update datawarehouse.cep_denovo
-- set nr_cep = concat('0',nr_cep,'000')
-- where length (nr_cep)= 4

-- update datawarehouse.cep_denovo
-- set nr_cep = concat('0',nr_cep)
-- where length (nr_cep)=7

-- update datawarehouse.cep_denovo
-- set nr_cep = '06814010'
-- where ds_endereco = 'RUA AUGUSTO DE ALMEIDA BATISTA 354'
-- AND length (nr_cep)=6

-- update datawarehouse.cep_denovo cdn
-- set nr_cep = gh.cep_inicio
-- from datawarehouse.tabela_cep_git_hub gh
-- where gh.cidade = cdn.nm_municipio
-- and gh.uf = cdn.sg_uf
-- and length (nr_cep)<8

-- update datawarehouse.cep_denovo cdn
-- set nr_cep = p2020.nr_cep
-- from public.tabela_cep_2020 p2020
-- where cdn.ds_endereco =  unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(p2020.ds_endereco, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g'))))
-- and cdn.sg_uf = p2020.sg_uf

-- SELECT distinct * FROM datawarehouse.cep_denovo
-- where nr_cep like '000%'
-- order by sg_uf

-- SELECT distinct * FROM datawarehouse.cep_denovo
-- where nr_cep like '00%'
-- order by sg_uf

-- update datawarehouse.cep_denovo
-- set  nr_cep  = 68880000
-- where nr_cep like '000000%'

-- update datawarehouse.tabela_cep_git_hub
-- set cidade  = unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(cidade, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g'))))

-- SELECT distinct * FROM datawarehouse.cep_denovo
-- where nr_cep is null

-- update datawarehouse.cep_denovo cdn
-- set nr_cep = cgh.cep_inicio
-- from datawarehouse.tabela_cep_git_hub cgh
-- where cdn.nr_cep is null
-- and cgh.uf = cdn.sg_uf
-- and cgh.cidade = cdn.nm_municipio

-- ALTER TABLE datawarehouse.dim_endereco
-- ADD COLUMN id SERIAL PRIMARY KEY;

12_12_2024
UPDATE datawarehouse.dim_cep dcep
set nr_latitude = dca.latitude
from datawarehouse.dim_cep_atualizado dca
where dca.nr_cep = dcep.nr_cep

UPDATE datawarehouse.dim_cep dcep
set nr_longitude = dca.longitude
from datawarehouse.dim_cep_atualizado dca
where dca.nr_cep = dcep.nr_cep

select nr_cep from datawarehouse.cep_denovo
where length(nr_cep)> 8

update datawarehouse.cep_denovo dcep
set nr_cep = regexp_replace(nr_cep, '-','' ,'g')

update datawarehouse.cep_denovo dcep
set nr_cep = concat('0',nr_cep)

--remove a primeira letra
UPDATE  datawarehouse.dim_cep dcep
SET nr_cep = SUBSTRING(nr_cep FROM 2)  -- Remove a primeira letra
WHERE LENGTH(nr_cep) > 8;

update datawarehouse.cep_denovo cdn
set nr_latitude = dca.latitude 
from datawarehouse.dim_cep_atualizado dca
where cdn.nr_cep = dca.nr_cep

update datawarehouse.dim_cep cdn
set nr_latitude = dca.latitude
from datawarehouse.dim_cep_atualizado dca
where cdn.nr_cep = dca.nr_cep

insert into datawarehouse.fato_cep (fk_dim_bairro,fk_dim_cep,fk_id_ano,fk_dim_zona,fk_dim_municipio,fk_dim_sg_uf)
select distinct
nm_bairro,
nr_cep,
aa_eleicao,
nr_zona,
nm_municipio,
sg_uf
FROM datawarehouse.cep_denovo


select distinct ds_local_votacao_endereco from public.eleicoes
where ds_local_votacao_endereco is not null
limit 100

ALTER TABLE datawarehouse.fato_eleicao
ADD COLUMN id SERIAL PRIMARY KEY;

alter table  datawarehouse.fato_eleicao
alter COLUMN qt_comparecimento TYPE integer using qt_comparecimento::integer

insert into datawarehouse.fato_eleicao (fk_dim_ano,fk_dim_sg_uf, fk_dim_cd_municipio,fk_dim_zona,fk_dim_turno,fk_nr_votavel,nm_votavel,
			qt_aptos,qt_comparecimento,qt_abstencoes,qt_votos_nominais,qt_votos)
			select distinct
			aa_eleicao ,
			sg_uf ,
			cd_municipio ,
			nr_zona ,
			nr_turno ,
			nr_votavel ,
			nm_votavel ,
			qt_aptos,
			qt_comparecimento,
			qt_abstencoes,
			qt_votos_nominais,
			qt_votos
			from public.eleicoes

-- update chaves dim MUNICIPIO
-- UPDATE datawarehouse.dim_municipio dm
-- SET fk_sg_uf = dsu.id
-- from datawarehouse.dim_sg_uf dsu
-- where dm.fk_sg_uf = dsu.sg_uf

-- update chaves dim BAIRRO
-- UPDATE  datawarehouse.dim_bairro dbr
-- SET fk_nr_cep = dcep.id
-- from datawarehouse.dim_cep dcep
-- where dcep.nr_cep = dbr.fk_sg_uf


-- UPDATE  datawarehouse.dim_bairro dbr
-- SET fk_cd_municipio = dm.id
-- from datawarehouse.dim_municipio dm
-- where dm.cd_municipio = dsu.sg_uf

-- UPDATE  datawarehouse.dim_bairro dbr
-- SET fk_nr_cep = dcep.id
-- from datawarehouse.dim_cep dcep 
-- where dcep.nr_cep = dbr.fk_nr_cep

-- update chaves dim_ENDERECO

-- UPDATE datawarehouse.dim_endereco
-- SET fk_nr_cep = dcep.id
-- FROM datawarehouse.dim_cep dcep
-- where fk_nr_cep = dcep.nr_cep

-- UPDATE datawarehouse.dim_endereco
-- SET fk_sg_uf = dsu.id
-- FROM datawarehouse.dim_sg_uf dsu
-- where fk_sg_uf = dsu.sg_uf

-- UPDATE datawarehouse.dim_endereco
-- SET fk_nm_bairro = dbr.id
-- FROM datawarehouse.dim_bairro dbr
-- where  fk_nm_bairro = dbr.nm_bairro

-- UPDATE datawarehouse.dim_endereco
-- SET  fk_cd_municipio = dm.id 
-- FROM datawarehouse.dim_municipio dm
-- where  fk_cd_municipio = dm.cd_municipio

-- update chaves dim_zona

-- update datawarehouse.dim_zona
-- set fk_cd_municipio = dm.id
-- from datawarehouse.dim_municipio dm
-- where fk_cd_municipio = dm.cd_municipio

-- update datawarehouse.dim_zona
-- set fk_sg_uf = dsu.id
-- from datawarehouse.dim_sg_uf dsu
-- where  fk_sg_uf = dsu.sg_uf

-- update datawarehouse.dim_zona
-- set fk_nr_cep = dcep.id
-- from datawarehouse.dim_cep dcep
-- where  fk_nr_cep = dcep.nr_cep

-- update datawarehouse.dim_zona
-- set fk_ds_endereco = de.id
-- from datawarehouse.dim_endereco de
-- where  fk_ds_endereco = de.ds_endereco

-- update datawarehouse.dim_zona
-- set fk_nm_bairro = dbr.id
-- from datawarehouse.dim_bairro dbr
-- where fk_nm_bairro = dbr.nm_bairro

--chaves da fato_cep

-- update datawarehouse.fato_cep
-- set fk_dim_cep  = dcep.id
-- from datawarehouse.dim_cep dcep
-- where fk_dim_cep = dcep.nr_cep

-- update datawarehouse.fato_cep
-- set fk_id_ano = da.id
-- from datawarehouse.dim_ano da
-- where fk_id_ano = da.ano

-- update datawarehouse.fato_cep
-- set fk_dim_zona = dz.id
-- from datawarehouse.dim_zona dz
-- where fk_dim_zona = dz.nr_zona

-- update datawarehouse.fato_cep
-- set fk_cd_municipio = dm.id
-- from datawarehouse.dim_municipio dm
-- where fk_cd_municipio = dm.cd_municipio

-- update datawarehouse.fato_cep
-- set  fk_dim_bairro = dbr.id
-- from datawarehouse.dim_bairro dbr
-- where  fk_dim_bairro = dbr.nm_bairro

-- update datawarehouse.fato_cep fcep
-- set  ds_endereco = de.id
-- from datawarehouse.dim_endereco de
-- where fcep.ds_endereco = de.ds_endereco

--chaves da fato_eleicao

-- update datawarehouse.fato_eleicao
-- set fk_dim_ano = da.id
-- from datawarehouse.dim_ano da
-- where fk_dim_ano = da.ano

-- update datawarehouse.fato_eleicao
-- set fk_dim_sg_uf = dsu.id
-- from datawarehouse.dim_sg_uf dsu
-- where fk_dim_sg_uf = dsu.sg_uf

-- update datawarehouse.fato_eleicao
-- set fk_dim_cd_municipio = dm.id
-- from datawarehouse.dim_municipio dm
-- where fk_dim_cd_municipio = dm.cd_municipio

-- update datawarehouse.fato_eleicao
-- set fk_dim_zona = dz.id
-- from datawarehouse.dim_zona dz
-- where fk_dim_zona = dz.nr_zona

-- update datawarehouse.fato_eleicao
-- set fk_dim_turno = dt.id
-- from datawarehouse.dim_turno dt
-- where fk_dim_turno = dt.nr_turno

13/12/2024

-- alter table datawarehouse.dim_bairro
-- alter column fk_nr_cep type integer using  fk_nr_cep::integer

-- select fk_nr_cep from datawarehouse.dim_bairro
-- where fk_nr_cep ilike '0%'

-- --tem cep errrado
-- select fk_nr_cep from datawarehouse.dim_bairro
-- where length(fk_nr_cep)= 7

-- --cep_denovo esta ok
-- select nr_cep from datawarehouse.cep_denovo
-- where length(nr_cep)< 8

-- --cepes_api_01 coreção em sp
-- alter table datawarehouse.cepes_api_01
-- alter column nr_cep type varchar using nr_cep ::varchar

-- update datawarehouse.cepes_api_01
-- set nr_cep = concat('0', nr_cep)
-- where length(nr_cep::text)= 7
-- and sg_uf = 'SP'

-- update datawarehouse.cepes_api_01
-- set nr_cep = concat( nr_cep,'00000')
-- where length(nr_cep::text)= 3

-- update datawarehouse.cepes_api_01
-- set nr_cep = concat( nr_cep,'00000')
-- where length(nr_cep::text)= 3

-- update datawarehouse.cepes_api_01
-- set nr_cep = '74340080'
-- where nr_cep = '502'

-- select nr_cep, sg_uf,ds_endereco from datawarehouse.cepes_api_01
-- where length(nr_cep::text)< 6
-- order by nr_cep desc

-- select nr_cep, sg_uf from datawarehouse.cepes_api_01
-- where length(nr_cep::text) <8
-- order by nr_cep desc

--cepes_api_02 coreção em sp

-- select nr_cep, sg_uf from datawarehouse.cepes_api_02
-- where length(nr_cep::text) <8
-- order by nr_cep desc

-- alter table datawarehouse.cepes_api_02
-- alter column nr_cep type varchar using nr_cep ::varchar

-- update datawarehouse.cepes_api_02
-- set nr_cep = concat( nr_cep,'0000')
-- where length(nr_cep::text) = 4

--cepes_api_04 coreção em sp

-- select nr_cep, sg_uf from datawarehouse.cepes_api_04
-- where length(nr_cep::text) < 8

-- atualizar dim_cep
 -- select distinct nr_cep, nr_latitude,nr_longitude
 -- from datawarehouse.cep_denovo
 -- order by nr_cep asc
 -- insert into datawarehouse.dim_cep (nr_cep,nr_latitude,nr_longitude)
 -- select distinct nr_cep, nr_latitude,nr_longitude
 -- from datawarehouse.cep_denovo

-- atualizar primeiro as chaves estrangeiras deixar em INT

--atualizar cep nas dimensões
--atualizar BAIRRO
-- alter table datawarehouse.dim_bairro
-- add column id serial primary key

-- UPDATE datawarehouse.dim_bairro dbr
-- SET nr_cep = dcep.id
-- FROM datawarehouse.dim_cep dcep
-- where dbr.nr_cep = dcep.nr_cep

-- UPDATE datawarehouse.dim_bairro dbr
-- SET cd_municipio = dm.id
-- FROM datawarehouse.dim_municipio dm
-- where dbr.cd_municipio = dm.cd_municipio

-- UPDATE datawarehouse.dim_bairro dbr
-- SET sg_uf = dsu.id
-- FROM datawarehouse.dim_sg_uf dsu
-- where dsu.sg_uf = dbr.sg_uf 

-- alter table datawarehouse.dim_bairro
-- alter column fk_sg_uf  type integer using fk_sg_uf ::integer

-- alter table datawarehouse.dim_bairro
-- alter column fk_nr_cep  type integer using fk_nr_cep ::integer

--ATUALIZAR ENDEREÇO

-- update datawarehouse.dim_endereco de
-- set nr_cep = dcep.id
-- from datawarehouse.dim_cep dcep
-- where dcep.nr_cep = de.nr_cep

-- update datawarehouse.dim_endereco de
-- set cd_municipio = dm.id
-- from datawarehouse.dim_municipio dm
-- where dm.cd_municipio = de.cd_municipio

-- update datawarehouse.dim_endereco de
-- set sg_uf = dsu.id
-- from datawarehouse.dim_sg_uf dsu
-- where dsu.sg_uf = de.sg_uf

-- update datawarehouse.dim_endereco de
-- set nm_bairro = dbr.id
-- from datawarehouse.dim_bairro dbr
-- where dbr.nm_bairro = de.nm_bairro

-- alter table datawarehouse.dim_endereco
-- add column id serial primary key

-- alter table datawarehouse.dim_endereco
-- alter column fk_nr_cep  type integer using fk_nr_cep ::integer

-- alter table datawarehouse.dim_endereco
-- alter column fk_sg_uf  type integer using fk_sg_uf ::integer

-- alter table datawarehouse.dim_endereco
-- alter column fk_nm_bairro  type integer using fk_nm_bairro ::integer

--ATUALIZAR ZONA

-- alter table datawarehouse.dim_zona
-- add column id serial primary key

-- update datawarehouse.dim_zona dz
-- set nr_cep = dcep.id
-- from datawarehouse.dim_cep dcep
-- where dcep.nr_cep = dz.nr_cep

-- update datawarehouse.dim_zona dz
-- set ds_endereco = de.id
-- from datawarehouse.dim_endereco de
-- where de.ds_endereco = dz.ds_endereco

-- update datawarehouse.dim_zona dz
-- set cd_municipio = dm.id
-- from datawarehouse.dim_municipio dm
-- where dz.cd_municipio = dm.cd_municipio

-- update datawarehouse.dim_zona dz
-- set sg_uf = dsu.id
-- from datawarehouse.dim_sg_uf dsu
-- where dsu.sg_uf = dz.sg_uf

-- update datawarehouse.dim_zona dz
-- set nm_bairro = dbr.id
-- from datawarehouse.dim_bairro dbr
-- where dbr.nm_bairro = dz.nm_bairro

-- alter table datawarehouse.dim_zona
-- alter column nr_cep  type integer using nr_cep ::integer

-- alter table datawarehouse.dim_zona
-- alter column ds_endereco  type integer using ds_endereco ::integer

-- alter table datawarehouse.dim_zona
-- alter column sg_uf  type integer using sg_uf ::integer

-- alter table datawarehouse.dim_zona
-- alter column nm_bairro  type integer using nm_bairro ::integer

14/12/2024

-- UPDATE datawarehouse.fato_eleicao fe 
-- set aa_eleicao = da.id
-- FROM datawarehouse.dim_ano da 
-- where da.ano = fe.aa_eleicao

-- UPDATE datawarehouse.fato_eleicao fe 
-- set sg_uf = dsu.id
-- FROM datawarehouse.dim_sg_uf dsu
-- where dsu.sg_uf = fe.sg_uf

-- UPDATE datawarehouse.fato_eleicao fe 
-- set cd_municipio = dm.id
-- FROM datawarehouse.dim_municipio dm
-- where dm.cd_municipio = fe.cd_municipio

-- UPDATE datawarehouse.fato_eleicao fe 
-- set nr_zona = dz.id
-- from datawarehouse.dim_zona dz
-- where dz.nr_zona = fe.nr_zona

-- UPDATE datawarehouse.fato_eleicao fe 
-- set nr_turno = dt.id
-- from datawarehouse.dim_turno dt	 
-- where dt.nr_turno = fe.nr_turno

-- UPDATE datawarehouse.fato_eleicao fe 
-- set nr_votavel = dl.id
-- from datawarehouse.dim_legenda dl	 
-- where dl.nr_votavel = fe.nr_votavel

-- UPDATE datawarehouse.fato_partido fp
-- set fk_dim_legenda = dl.id
-- from datawarehouse.dim_legenda dl
-- where dl.nr_votavel= fp.nr_partido

select * from datawarehouse.fato_partido
where fk_dim_legenda is null

select * from datawarehouse.fato_eleicao
where nr_votavel is null


alter table datawarehouse.dim_municipio
alter column fk_sg_uf type integer using fk_sg_uf::integer

select nr_votavel,nm_votavel from datawarehouse.fato_eleicao
where nm_votavel like'JOAO FRANCISCO ALBUQUERQUE DE OLIVEIRA'


update datawarehouse.dim_nome_candidato
set fk_dim_legenda =  dl.id
from datawarehouse.dim_legenda dl
where dl.nr_votavel = fk_dim_legenda


alter table datawarehouse.fato_cep
alter COLUMN fk_nr_cep type integer using fk_nr_cep::integer

alter table datawarehouse.fato_cep
alter COLUMN fk_nm_bairro type integer using fk_nm_bairro::integer

alter table datawarehouse.fato_cep
alter COLUMN fk_sg_uf type integer using fk_sg_uf::integer


alter table datawarehouse.fato_eleicao
alter COLUMN fk_sg_uf type integer using fk_sg_uf::integer

SELECT * FROM datawarehouse.fato_partido
where fk_dim_legenda is null

insert into datawarehouse.dim_legenda (nr_votavel)
values (97),(98),(99)

update datawarehouse.fato_partido
set fk_dim_legenda = dl.id
from datawarehouse.dim_legenda dl
where fk_dim_legenda is null
and nr_votavel = dl.nr_votavel

CONSULTA TOP!!!! PRO Bi  I!!
select distinct 
	da.ano,
	dt.nr_turno,
	dl.nr_votavel,
	fe.nm_votavel,
	dsu.sg_uf,
	dm.nm_municipio,
	fe.fk_nr_zona,
	sum(fe.qt_votos) AS total_votos
	FROM  datawarehouse.fato_eleicao fe
INNER JOIN datawarehouse.dim_ano da ON da.id = fe.fk_aa_eleicao
INNER JOIN datawarehouse.dim_turno dt ON dt.id = fe.fk_nr_turno	
INNER JOIN datawarehouse.dim_legenda dl ON dl.id = fe.fk_nr_votavel
INNER JOIN datawarehouse.dim_sg_uf dsu ON dsu.id = fe.fk_sg_uf
INNER JOIN datawarehouse.dim_municipio dm ON dm.id = fe.fk_cd_municipio
INNER JOIN datawarehouse.dim_zona dz ON dz.id = fe.fk_nr_zona 
WHERE dsu.sg_uf ilike 'CE' 
AND da.ano = 2016
aND dt.nr_turno = 1 
AND dm.nm_municipio ilike'Fortaleza'
GROUP BY da.ano, dt.nr_turno, dl.nr_votavel, fe.nm_votavel, dsu.sg_uf, dm.nm_municipio, fe.fk_nr_zona, fe.nm_votavel
ORDER BY (sum(fe.qt_votos)) DESC
LIMIT 100;

select distinct 
da.ano,
dcep.nr_cep,
dm.cd_municipio,
dsu.sg_uf,
dbr.nm_bairro,
dz.nr_zona
from datawarehouse.fato_cep fcep
inner join datawarehouse.dim_cep as dcep on dcep.id = fcep.fk_nr_cep
INNER JOIN datawarehouse.dim_ano as da on da.id = fcep.fk_aa_eleicao
INNER JOIN datawarehouse.dim_zona as dz on dz.id = fcep.fk_nr_zona
INNER JOIN datawarehouse.dim_bairro as dbr on dbr.id = fcep.fk_nm_bairro
INNER JOIN datawarehouse.dim_municipio as dm on dm.id = fcep.fk_cd_municipio
INNER JOIN datawarehouse.dim_sg_uf as dsu on dsu.id = fcep.fk_sg_uf

15_12_2024

--consulta 01
select distinct 
da.ano,
dcep.nr_cep,
dm.cd_municipio,
dsu.sg_uf,
dbr.nm_bairro,
dz.nr_zona
from datawarehouse.fato_cep fcep
inner join datawarehouse.dim_cep as dcep on dcep.id = fcep.fk_nr_cep
INNER JOIN datawarehouse.dim_ano as da on da.id = fcep.fk_aa_eleicao
INNER JOIN datawarehouse.dim_zona as dz on dz.id = fcep.fk_nr_zona
INNER JOIN datawarehouse.dim_bairro as dbr on dbr.id = fcep.fk_nm_bairro
INNER JOIN datawarehouse.dim_municipio as dm on dm.id = fcep.fk_cd_municipio
INNER JOIN datawarehouse.dim_sg_uf as dsu on dsu.id = fcep.fk_sg_uf

--consulta 02
select distinct 
	da.ano,
	dt.nr_turno,
	dl.nr_votavel,
	fe.nm_votavel,
	dsu.sg_uf,
	dp.nome_pais,
	dm.nm_municipio,
	dbr.nm_bairro,
	dcep.nr_cep,
	dcep.nr_latitude,
	dcep.nr_longitude,
	fe.fk_nr_zona,
	sum(fe.qt_votos) AS total_votos
	FROM  datawarehouse.fato_eleicao fe
INNER JOIN datawarehouse.dim_ano da ON da.id = fe.fk_aa_eleicao
INNER JOIN datawarehouse.dim_turno dt ON dt.id = fe.fk_nr_turno	
INNER JOIN datawarehouse.dim_legenda dl ON dl.id = fe.fk_nr_votavel
INNER JOIN datawarehouse.dim_sg_uf dsu ON dsu.id = fe.fk_sg_uf
INNER JOIN datawarehouse.dim_municipio dm ON dm.id = fe.fk_cd_municipio
INNER JOIN datawarehouse.dim_zona dz ON dz.id = fe.fk_nr_zona
inner join datawarehouse.dim_bairro dbr on dbr.id = dz.fk_nm_bairro
inner join datawarehouse.dim_cep dcep on dcep.id = dz.fk_nr_cep
inner join datawarehouse.dim_pais dp on dp.id = dsu.fk_pais
GROUP BY da.ano, dt.nr_turno, dl.nr_votavel, fe.nm_votavel, dsu.sg_uf, dm.nm_municipio,dbr.nm_bairro,
		 fe.fk_nr_zona,dcep.nr_cep,dcep.nr_latitude, dcep.nr_longitude, fe.nm_votavel,dp.nome_pais
ORDER BY (sum(fe.qt_votos)) DESC

-- consulta 03
select distinct 
	da.ano,
	dt.nr_turno,
	dl.nr_votavel,
	fe.nm_votavel,
	dsu.sg_uf,
	dm.nm_municipio,
	dbr.nm_bairro,
	fe.fk_nr_zona,
	sum(fe.qt_votos) AS total_votos
	FROM  datawarehouse.fato_eleicao fe
INNER JOIN datawarehouse.dim_ano da ON da.id = fe.fk_aa_eleicao
INNER JOIN datawarehouse.dim_turno dt ON dt.id = fe.fk_nr_turno	
INNER JOIN datawarehouse.dim_legenda dl ON dl.id = fe.fk_nr_votavel
INNER JOIN datawarehouse.dim_sg_uf dsu ON dsu.id = fe.fk_sg_uf
INNER JOIN datawarehouse.dim_municipio dm ON dm.id = fe.fk_cd_municipio
INNER JOIN datawarehouse.dim_zona dz ON dz.id = fe.fk_nr_zona
inner join datawarehouse.dim_bairro dbr on dbr.id = dz.fk_nm_bairro
WHERE dsu.sg_uf ilike 'CE' 
AND da.ano = 2016
aND dt.nr_turno = 1 
AND dm.nm_municipio ilike'Fortaleza'
GROUP BY da.ano, dt.nr_turno, dl.nr_votavel, fe.nm_votavel, dsu.sg_uf, dm.nm_municipio,dbr.nm_bairro, fe.fk_nr_zona, fe.nm_votavel
ORDER BY (sum(fe.qt_votos)) DESC
LIMIT 100;

03/01/2011
CREATE TABLE datawarehouse.dim_tabela_bi AS
SELECT 
    de.ds_endereco,
    dbr.nm_bairro,
    dm.nm_municipio,
    dsu.estado,
    dp.nome_pais,
    dcep.nr_cep,
    -- Concatenação 1: Estado e País
    TRIM(BOTH ',' FROM CONCAT(
        dsu.estado, ', ',
        dp.nome_pais
    )) AS endereco_completo_1,
    -- Concatenação 2: Município, Estado e País
    TRIM(BOTH ',' FROM CONCAT(
        dm.nm_municipio, ', ',
        dsu.estado, ', ',
        dp.nome_pais
    )) AS endereco_completo_2,
    -- Concatenação 3: Bairro, Município, Estado e País
    TRIM(BOTH ',' FROM CONCAT(
        dbr.nm_bairro, ', ',
        dm.nm_municipio, ', ',
        dsu.estado, ', ',
        dp.nome_pais
    )) AS endereco_completo_3,
    -- Concatenação Completa: Endereço, Bairro, Município, Estado e País
    TRIM(BOTH ',' FROM CONCAT(
        de.ds_endereco, ', ',
        dbr.nm_bairro, ', ',
        dm.nm_municipio, ', ',
        dsu.estado, ', ',
        dp.nome_pais
    )) AS endereco_completo
FROM datawarehouse.dim_endereco de
INNER JOIN datawarehouse.dim_cep AS dcep ON dcep.id = de.fk_nr_cep
INNER JOIN datawarehouse.dim_municipio AS dm ON dm.id = de.fk_cd_municipio
INNER JOIN datawarehouse.dim_sg_uf AS dsu ON dsu.id = de.fk_sg_uf
INNER JOIN datawarehouse.dim_bairro AS dbr ON dbr.id = de.fk_nm_bairro
INNER JOIN datawarehouse.dim_pais AS dp ON dp.id = dsu.fk_pais;


ALTER TABLE datawarehouse.dim_sg_uf
ADD COLUMN estado VARCHAR(100);

UPDATE datawarehouse.dim_sg_uf
SET estado = CASE 
    WHEN sg_uf = 'AC' THEN 'Acre'
    WHEN sg_uf = 'AL' THEN 'Alagoas'
    WHEN sg_uf = 'AM' THEN 'Amazonas'
    WHEN sg_uf = 'AP' THEN 'Amapa'
    WHEN sg_uf = 'BA' THEN 'Bahia'
    WHEN sg_uf = 'CE' THEN 'Ceara'
    WHEN sg_uf = 'DF' THEN 'Distrito Federal'
    WHEN sg_uf = 'ES' THEN 'Espirito Santo'
    WHEN sg_uf = 'GO' THEN 'Goias'
    WHEN sg_uf = 'MA' THEN 'Maranhao'
    WHEN sg_uf = 'MG' THEN 'Minas Gerais'
    WHEN sg_uf = 'MS' THEN 'Mato Grosso do Sul'
    WHEN sg_uf = 'MT' THEN 'Mato Grosso'
    WHEN sg_uf = 'PA' THEN 'Para'
    WHEN sg_uf = 'PB' THEN 'Paraiba'
    WHEN sg_uf = 'PE' THEN 'Pernambuco'
    WHEN sg_uf = 'PI' THEN 'Piaui'
    WHEN sg_uf = 'PR' THEN 'Parana'
    WHEN sg_uf = 'RJ' THEN 'Rio de Janeiro'
    WHEN sg_uf = 'RN' THEN 'Rio Grande do Norte'
    WHEN sg_uf = 'RO' THEN 'Rondonia'
    WHEN sg_uf = 'RR' THEN 'Roraima'
    WHEN sg_uf = 'RS' THEN 'Rio Grande do Sul'
    WHEN sg_uf = 'SC' THEN 'Santa Catarina'
    WHEN sg_uf = 'SE' THEN 'Sergipe'
    WHEN sg_uf = 'SP' THEN 'Sao Paulo'
    WHEN sg_uf = 'TO' THEN 'Tocantins'
    ELSE NULL -- Opcional
END;

06/01/2024

SELECT * FROM datawarehouse.dim_bairro
ORDER BY fk_cd_municipio
296593

DELETE FROM  datawarehouse.dim_municipio
WHERE id BETWEEN 296598 AND 307847;

SELECT * FROM datawarehouse.dim_municipio
order by id
where cd_municipio = 86460


--Atualizar as Tabelas Relacionadas
UPDATE datawarehouse.dim_zona AS fato
SET fk_cd_municipio = (
    SELECT MIN(id)
    FROM datawarehouse.dim_municipio AS dim
    WHERE fato.fk_cd_municipio = dim.id
    GROUP BY nm_municipio, fk_sg_uf, cd_municipio
)
WHERE fk_cd_municipio IN (
    SELECT id
    FROM datawarehouse.dim_municipio
    WHERE id NOT IN (
        SELECT MIN(id)
        FROM datawarehouse.dim_municipio
        GROUP BY nm_municipio, fk_sg_uf, cd_municipio
    )
);



-- SQL para Remover Duplicatas
DELETE FROM datawarehouse.dim_municipio
WHERE id NOT IN (
    SELECT MIN(id)
    FROM datawarehouse.dim_municipio
    GROUP BY 
        nm_municipio, 
        fk_sg_uf, 
        cd_municipio
);
--Identificar Duplicatas
SELECT 
    nm_municipio, 
    fk_sg_uf, 
    cd_municipio, 
    MIN(id) AS menor_id, 
    COUNT(*) AS total
FROM 
    datawarehouse.dim_municipio
GROUP BY 
    nm_municipio, 
    fk_sg_uf, 
    cd_municipio
HAVING 
    COUNT(*) > 1;
	


UPDATE datawarehouse.dim_municipio AS dm
SET cd_municipio = fe.cd_municipio
from datawarehouse.cep_denovo fe
where fe.nm_municipio = dm.nm_municipio AND 
dm.cd_municipio is null

UPDATE datawarehouse.dim_municipio AS dm
SET 
    nm_municipio = SPLIT_PART(dm.estado_municipio, ',', 1),
    fk_sg_uf = (
        SELECT id
        FROM datawarehouse.dim_sg_uf AS dsu
        WHERE dsu.estado = TRIM(SPLIT_PART(dm.estado_municipio, ',', 2))
    )
WHERE dm.nm_municipio IS NULL;



SELECT nm_municipio FROM datawarehouse.dim_municipio
where nm_municipio ilike 'nova fatima'
order by id




select * from datawarehouse.fato_partido
order by id

DELETE FROM  datawarehouse.fato_partido
WHERE id BETWEEN 39 AND 76;
-- ELEITOS E NÃO ELEITOS EM PRIMEIRO TURNO

WITH total_votos_1_turno_municipio AS (
  SELECT 
    dsu.estado AS estado,
    dm.nm_municipio AS municipio,
    da.ano AS ano,
    SUM(fe.qt_votos) AS total_votos
  FROM datawarehouse.fato_eleicao fe
  JOIN datawarehouse.dim_legenda dl ON dl.id = fe.fk_nr_votavel
  JOIN datawarehouse.dim_municipio dm ON dm.id = fe.fk_cd_municipio
  JOIN datawarehouse.dim_ano da ON da.id = fe.fk_aa_eleicao
  JOIN datawarehouse.dim_turno dt ON dt.id = fe.fk_nr_turno
  JOIN datawarehouse.dim_sg_uf dsu ON dsu.id = fe.fk_sg_uf
  WHERE dt.nr_turno = 1
  GROUP BY dsu.estado, dm.nm_municipio, da.ano
),
qtd_votos_partido_ano AS (
  SELECT 
    da.ano AS ano,
    dsu.estado AS estado,
    dm.nm_municipio AS municipio,
    dl.nr_votavel AS partido,
    SUM(fe.qt_votos) AS votos_partido
  FROM datawarehouse.fato_eleicao fe
  JOIN datawarehouse.dim_legenda dl ON dl.id = fe.fk_nr_votavel
  JOIN datawarehouse.dim_municipio dm ON dm.id = fe.fk_cd_municipio
  JOIN datawarehouse.dim_ano da ON da.id = fe.fk_aa_eleicao
  JOIN datawarehouse.dim_turno dt ON dt.id = fe.fk_nr_turno
  JOIN datawarehouse.dim_sg_uf dsu ON dsu.id = fe.fk_sg_uf
  WHERE dt.nr_turno = 1
  GROUP BY da.ano, dsu.estado, dm.nm_municipio, dl.nr_votavel
)
SELECT 
  q.ano,
  q.estado,
  q.municipio,
  q.partido,
  q.votos_partido,
  t.total_votos,
  ROUND(q.votos_partido::numeric * 100 / t.total_votos, 2) AS percentual_votos,
  CASE
    WHEN ROUND(q.votos_partido::numeric * 100 / t.total_votos, 2) > 50 THEN true
    ELSE false
  END AS eleito_1_turno
FROM qtd_votos_partido_ano q
JOIN total_votos_1_turno_municipio t
  ON q.ano = t.ano
  AND q.estado = t.estado
  AND q.municipio = t.municipio
ORDER BY eleito_1_turno DESC, percentual_votos DESC;








			




















* Logico_eleicoes: */

13 -

--ok 
CREATE TABLE datawarehouse.dim_nome_candidato (
    id_nome_candidato serial PRIMARY KEY,
    nome_candidato varchar,
    fk_id_partido integer REFERENCES datawarehouse.dim_partidos(id_partido)
);


--ok
CREATE TABLE datawarehouse.dim_partidos (
    id_partido serial PRIMARY KEY,
    legenda integer
);

5-
--ok
CREATE TABLE datawarehouse.dim_estado (
    id_estado serial PRIMARY KEY UNIQUE,
    nome varchar
);
6-
8-
--ok
CREATE TABLE datawarehouse.dim_municipio (
    id_municipio serial PRIMARY KEY UNIQUE,
    nome_mun varchar,
    cod_mun integer,
    fk_id_estado integer REFERENCES datawarehouse.dim_estado(id_estado)
);
1- 
--ok
CREATE TABLE datawarehouse.dim_turno (
    id_turno serial PRIMARY KEY,
    tipo serial
);

2-
--ok
CREATE TABLE datawarehouse.dim_data_eleicao (
    id serial PRIMARY KEY UNIQUE,
    dia date,
    mes date,
    ano date
);

– SELECT DISTINCT
–   EXTRACT(YEAR FROM dt_eleicao):: INT AS ano,
–  EXTRACT(MONTH FROM dt_eleicao)::INT AS mes,
–    EXTRACT(DAY FROM dt_eleicao):: INT AS dia
– FROM "public".eleicoes;


3-
--ok
CREATE TABLE datawarehouse.dim_dsc_eleicao (
    id serial PRIMARY KEY UNIQUE,
    nome varchar
);
4-


CREATE TABLE datawarehouse.dim_cep (
 id_cep serial PRIMARY KEY,
cep integer
 NR_LATITUDE BIGINT,
 NR_LONGITUDE BIGINT
);



7-
 
CREATE TABLE datawarehouse.fato_eleicoes (
    id serial PRIMARY KEY UNIQUE,
    fk_id_turno integer REFERENCES datawarehouse.dim_turno(id_turno),
    fk_id_data integer REFERENCES datawarehouse.dim_data_eleicao(id_data) , 
	fk_id_local_votacao integer REFERENCES datawarehouse.dim_local_votacao(id_local_votacao) ,
    fk_id_nome_candidato integer REFERENCES datawarehouse.dim_nome_candidato(id_nome_candidato),
    qtd_votos integer,
	qtd_comparecimentos integer,
    qtd_abstencoes integer,
    qtd_aptos integer
);

--ok
CREATE TABLE datawarehouse.dim_zonas_eleitorais (
    id_zona serial PRIMARY KEY UNIQUE,
    numero_zona integer,
    fk_id_estado integer REFERENCES datawarehouse.dim_estado(id_estado) ,
    fk_id_municipio integer REFERENCES datawarehouse.dim_municipio(id_municipio)
);
11-
--ok
CREATE TABLE datawarehouse.dim_local_votacao (
    id_local_votacao serial PRIMARY KEY,
    nome_local varchar,
    fk_id_logradouro integer REFERENCES datawarehouse.dim_logradouro(id_logradouro) ,
    fk_id_secao integer REFERENCES datawarehouse.dim_secao(id_secao),
    fk_id_cep integer references datawarehouse.dim_cep(id_cep),
    fk_id_municipio integer REFERENCES datawarehouse.dim_municipio(id_municipio) ,
	numero integer
);

12 -
--ok
CREATE TABLE datawarehouse.dim_secao (
    id_secao serial PRIMARY KEY,
    numero_secao integer,
    fk_id_municipio integer REFERENCES datawarehouse.dim_municipio(id_municipio),
    fk_id_logradouro integer REFERENCES datawarehouse.dim_logradouro(id_logradouro)
);



 23_11_2024
SELECT DISTINCT EXTRACT(YEAR FROM dt_eleicao) AS ano_eleicao,
nr_votavel as legenda
FROM public.eleicoes
order by legenda

ALTER TABLE datawarehouse.dim_cep
ADD COLUMN nr_latitude BIGINT,
ADD COLUMN nr_longitude BIGINT;

select nr_local_votacao,
nm_local_votacao,
nr_secao,
sg_uf
from public.eleicoes_novo
where nr_local_votacao = 1147
and sg_uf = 'BA'
and nm_local_votacao ilike 'escola%'
order by nm_local_votacao

select * 
from datawarehouse.dim_cep
order by nr_cep



 24/11/2024

ALTER TABLE public."tabela cep geral"
ALTER COLUMN nr_latitude TYPE double precision USING nr_latitude::double precision;

ALTER TABLE public."tabela cep geral"
ALTER COLUMN nr_longitude TYPE double precision USING nr_longitude::double precision;

ALTER TABLE public."tabela cep geral"
ALTER COLUMN nm_municipio TYPE character varying(50);

ALTER TABLE public."tabela cep geral"
ALTER COLUMN nm_local_votacao TYPE character varying(100);

ALTER TABLE public."tabela cep geral"
ALTER COLUMN nm_local_votacao_original TYPE character varying(100);

ALTER TABLE public."tabela cep geral"
ALTER COLUMN ds_endereco TYPE character varying(100);

ALTER TABLE public."tabela cep geral"
ALTER COLUMN ds_endereco_locvt_original TYPE character varying(100);

ALTER TABLE public."tabela cep geral"
ALTER COLUMN nm_bairro TYPE character varying(100);





COPY public."tabela cep geral" (
    hh_geracao, aa_eleicao, dt_eleicao, ds_eleicao, nr_turno, sg_uf, cd_municipio, nm_municipio, 
    nr_zona, nr_secao, cd_tipo_secao_agregada, ds_tipo_secao_agregada, nr_secao_principal, nr_local_votacao, 
    nm_local_votacao, cd_tipo_local, ds_tipo_local, ds_endereco, nm_bairro, nr_cep, nr_telefone_local, 
    nr_latitude, nr_longitude, cd_situ_local_votacao, ds_situ_local_votacao, cd_situ_zona, ds_situ_zona, 
    cd_situ_secao, ds_situ_secao, cd_situ_localidade, ds_situ_localidade, cd_situ_secao_acessibilidade, 
    ds_situ_secao_acessibilidade, qt_eleitor_secao, qt_eleitor_eleicao_federal, qt_eleitor_eleicao_estadual, 
    qt_eleitor_eleicao_municipal, nr_local_votacao_original, nm_local_votacao_original, ds_endereco_locvt_original
)
FROM 'E:\PYTHON\CURSOS\10_Digital_College\ETL\projeto etl\zonas_estado\eleitorado_local_votacao_2024_corrigido_formatado.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ';',
    ENCODING 'ISO-8859-1',
    QUOTE '"',
    ESCAPE ''''
);

select DISTINCT
 nr_zona, cd_municipio, nm_municipio, sg_uf, nr_cep, nr_latitude, nr_longitude, "tabela_cep_2024".aa_eleicao
 from public."tabela_cep_2024"
order by sg_uf

select distinct aa_eleicao from public."tabela_cep_2024"


SELECT DISTINCT ds_eleicao from public.eleicoes
SELECT * from public.eleicoes
where ds_eleicao = 'Eleições Municipais 2020 - AP'

-- Inicialmente programadas para os dias 4 e 25 de outubro, 
-- as eleições municipais em todo o país foram transferidas para
-- 15 de novembro (primeiro turno) e 29 de novembro (segundo turno)
-- devido à pandemia de COVID-19. Porém, uma crise elétrica no estado 
-- do Amapá, que começou na primeira semana de novembro, obrigou a um novo adiamento 
-- das eleições para 6 e 20 de dezembro, respectivamente, por
-- falta de condições de segurança, materiais e técnicas. 

SELECT * from datawarehouse.dim_data_eleicao
order by ano

UPDATE datawarehouse.dim_ds_eleicao
SET ds_eleicao = 'eleicao municipal 2012'  
WHERE ds_eleicao = 'ELEIÇÃO MUNICIPAL 2012'; 

UPDATE datawarehouse.dim_ds_eleicao
SET ds_eleicao = 'eleicao municipais 2016'  
WHERE ds_eleicao = 'Eleicões Municipais 2016'; 

UPDATE datawarehouse.dim_ds_eleicao
SET ds_eleicao = 'eleicao municipais 2020'  
WHERE ds_eleicao = 'Eleicões Municipais 2020'; 

UPDATE datawarehouse.dim_ds_eleicao
SET ds_eleicao = 'eleicao municipais 2020 - ap'  
WHERE ds_eleicao = 'Eleicões Municipais 2020 - AP'; 

select * from datawarehouse.dim_ds_eleicao

CREATE EXTENSION IF NOT EXISTS unaccent;

UPDATE datawarehouse.dim_municipio
SET nm_municipio = lower(replace(unaccent(nm_municipio), 'ç', 'c'));

ALTER TABLE datawarehouse.dim_municipio
ADD COLUMN fk_id_sg_uf INT;

UPDATE datawarehouse.dim_municipio 
SET fk_id_sg_uf = subquery.id
FROM (
    SELECT 
        dm.cd_municipio,
        su.id
    FROM 
        datawarehouse.dim_municipio dm
    INNER JOIN public.eleicoes pe ON pe.cd_municipio = dm.cd_municipio
    INNER JOIN datawarehouse.dim_sg_uf su ON pe.sg_uf = su.sg_uf
) AS subquery
WHERE datawarehouse.dim_municipio.cd_municipio = subquery.cd_municipio;


SELECT * FROM datawarehouse.dim_municipio
where nm_municipio ilike 'sao%'

SELECT * FROM datawarehouse.dim_sg_uf
where dim_sg_uf.id = 51

ALTER TABLE datawarehouse.dim_municipio
ADD CONSTRAINT fk_dim_municipio_sg_uf
FOREIGN KEY (fk_id_sg_uf)
REFERENCES datawarehouse.dim_sg_uf (id)
ON DELETE CASCADE;



--adiconar fk no nomce candidato da dim_eleicao
ALTER TABLE datawarehouse.dim_nome_candidato
ADD COLUMN fk_id_dim_legenda INT;

UPDATE datawarehouse.dim_nome_candidato
SET fk_id_dim_legenda = subquery.id
FROM (select distinct
		dl.id,
		dnc.legenda,
		dl.nr_votavel
		FROM datawarehouse.dim_legenda dl
		inner join datawarehouse.dim_nome_candidato as dnc on dnc.legenda = dl.nr_votavel
	)as subquery
WHERE datawarehouse.dim_nome_candidato.legenda = subquery.legenda;

UPDATE datawarehouse.dim_nome_candidato
SET fk_id_dim_legenda = NULL;

UPDATE datawarehouse.dim_nome_candidato
SET legenda = NULL;

UPDATE datawarehouse.dim_nome_candidato dnc
SET legenda = subquery.nr_votavel
FROM (
    SELECT DISTINCT
        pe.nr_votavel,
        dnc.nm_votavel
    FROM public.eleicoes pe
    INNER JOIN datawarehouse.dim_nome_candidato dnc ON dnc.nm_votavel = pe.nm_votavel
) AS subquery
WHERE dnc.nm_votavel = subquery.nm_votavel;

select distinct nr_votavel from public.eleicoes

ALTER TABLE datawarehouse.dim_nome_candidato
ADD CONSTRAINT fk_dim_legenda
FOREIGN KEY (fk_id_dim_legenda)
REFERENCES datawarehouse.dim_legenda (id)
ON DELETE CASCADE;




27/11/2024

select distinct
nr_zona,
dsu.id as fk_id_sgu_uf,
dsu.sg_uf as estado,
dm.id as fk_id_municipio,
dm.nm_municipio as municipio

from public.eleicoes as pe
inner join datawarehouse.dim_municipio as dm on dm.cd_municipio = pe.cd_municipio
inner join datawarehouse.dim_sg_uf as dsu on dsu.sg_uf = pe.sg_uf
order by estado

CREATE TABLE datawarehouse.dim_zonas_eleitorais (
    id SERIAL PRIMARY KEY,
    fk_id_sgu_uf INTEGER NOT NULL REFERENCES datawarehouse.dim_sg_uf (id),
    fk_id_municipio INTEGER NOT NULL REFERENCES datawarehouse.dim_municipio (id),
    nr_zona INTEGER NOT NULL,
    municipio TEXT NOT NULL,
    estado CHAR(2) NOT NULL
);

INSERT INTO datawarehouse.dim_zonas_eleitorais (fk_id_sgu_uf, fk_id_municipio, nr_zona, municipio, estado)
SELECT DISTINCT
    dsu.id AS fk_id_sgu_uf,
    dm.id AS fk_id_municipio,
    pe.nr_zona,
    dm.nm_municipio AS municipio,
    dsu.sg_uf AS estado
FROM public.eleicoes AS pe
INNER JOIN datawarehouse.dim_municipio AS dm ON dm.cd_municipio = pe.cd_municipio
INNER JOIN datawarehouse.dim_sg_uf AS dsu ON dsu.sg_uf = pe.sg_uf
ORDER BY estado;

CREATE TABLE datawarehouse.dim_logradouro (
    id serial PRIMARY KEY UNIQUE,
    logradouro varchar,
    fk_id_municipio integer REFERENCES datawarehouse.dim_municipio(id)
);

-- enderço contem nulos
select distinct 
pe.ds_local_votacao_endereco as endereco,
dsu.sg_uf AS estado,
dm.nm_municipio AS municipio

FROM public.eleicoes AS pe
INNER JOIN datawarehouse.dim_municipio AS dm ON dm.cd_municipio = pe.cd_municipio
INNER JOIN datawarehouse.dim_sg_uf AS dsu ON dsu.sg_uf = pe.sg_uf
ORDER BY endereco;

dsu.id AS fk_id_sgu_uf,
dm.id AS fk_id_municipio,
dze.nr_zona
INNER JOIN datawarehouse.dim_zonas_eleitorais AS dze ON dze.nr_zona = pe.nr_zona

-- fazer o selct dos ceps
-- fazendo um union 
SELECT DISTINCT
    sg_uf,
    cd_municipio,
    nm_municipio,
    nr_zona,
    nm_local_votacao,
    nr_secao,
    ds_endereco,
    nm_bairro,
    nr_cep,
    nr_latitude,
    nr_longitude
FROM public.tabela_cep_2020

UNION

SELECT DISTINCT
    sg_uf,
    cd_municipio,
    nm_municipio,
    nr_zona,
    nm_local_votacao,
    nr_secao,
    ds_endereco,
    nm_bairro,
    nr_cep,
    nr_latitude,
    nr_longitude
FROM public.tabela_cep_2016

UNION

SELECT DISTINCT
    sg_uf,
    cd_municipio,
    nm_municipio,
    nr_zona,
    nm_local_votacao,
    nr_secao,
    ds_endereco,
    nm_bairro,
    nr_cep,
    nr_latitude,
    nr_longitude
FROM public.tabela_cep_2012;

-- comparacao para saber se  houve modificaçao no cep

SELECT
    sg_uf,
    cd_municipio,
    nm_municipio,
    nr_zona,
    nm_local_votacao,
    nr_secao,
    ds_endereco,
    nm_bairro,
    nr_cep,
    nr_latitude,
    nr_longitude,
    '2020' AS ano
FROM public.tabela_cep_2020

UNION ALL

SELECT
    sg_uf,
    cd_municipio,
    nm_municipio,
    nr_zona,
    nm_local_votacao,
    nr_secao,
    ds_endereco,
    nm_bairro,
    nr_cep,
    nr_latitude,
    nr_longitude,
    '2016' AS ano
FROM public.tabela_cep_2016

UNION ALL

SELECT
    sg_uf,
    cd_municipio,
    nm_municipio,
    nr_zona,
    nm_local_votacao,
    nr_secao,
    ds_endereco,
    nm_bairro,
    nr_cep,
    nr_latitude,
    nr_longitude,
    '2012' AS ano
FROM public.tabela_cep_2012;

SELECT
    COALESCE(a.sg_uf, b.sg_uf) AS sg_uf,
    COALESCE(a.cd_municipio, b.cd_municipio) AS cd_municipio,
    COALESCE(a.nm_municipio, b.nm_municipio) AS nm_municipio,
    a.nr_cep AS cep_2020,
    b.nr_cep AS cep_2016,
    CASE
        WHEN a.nr_cep IS DISTINCT FROM b.nr_cep THEN 'CEP Alterado'
        WHEN a.nr_cep IS NULL THEN 'CEP Removido'
        WHEN b.nr_cep IS NULL THEN 'CEP Adicionado'
        ELSE 'Sem Alterações'
    END AS status
FROM public.tabela_cep_2020 a
FULL OUTER JOIN public.tabela_cep_2016 b
ON a.cd_municipio = b.cd_municipio
   AND a.nr_zona = b.nr_zona
   AND a.nm_local_votacao = b.nm_local_votacao;

--fazer dimensões cep por ano
-- criar tablea cep 2020
CREATE TABLE datawarehouse.dim_cep_2020 (
    id SERIAL PRIMARY KEY,
    fk_id_ano INTEGER NOT NULL REFERENCES datawarehouse.dim_ano(id),
    fk_id_nr_zona INTEGER NOT NULL REFERENCES datawarehouse.dim_zonas_eleitorais(id),
    nr_cep CHAR(8) NOT NULL,
    nr_latitude NUMERIC,
    nr_longitude NUMERIC
);


-- Inserir os dados de 2020
INSERT INTO datawarehouse.dim_cep_2020 (
    fk_id_ano,
    fk_id_nr_zona,
    nr_cep,
    nr_latitude,
    nr_longitude
)
SELECT distinct
    da.id AS fk_id_ano,
    dze.id AS fk_id_nr_zona,
    tc2020.nr_cep,
    tc2020.nr_latitude,
    tc2020.nr_longitude
FROM public.tabela_cep_2020 tc2020
INNER JOIN datawarehouse.dim_ano da ON da.ano = tc2020.aa_eleicao
INNER JOIN datawarehouse.dim_zonas_eleitorais dze ON dze.nr_zona = tc2020.nr_zona;

-- criar tablea cep 2016
CREATE TABLE datawarehouse.dim_cep_2016 (
    id SERIAL PRIMARY KEY,
    fk_id_ano INTEGER NOT NULL REFERENCES datawarehouse.dim_ano(id),
    fk_id_nr_zona INTEGER NOT NULL REFERENCES datawarehouse.dim_zonas_eleitorais(id),
    nr_cep CHAR(8) NOT NULL,
    nr_latitude NUMERIC,
    nr_longitude NUMERIC
);


-- Inserir os dados de 2016
INSERT INTO datawarehouse.dim_cep_2016 (
    fk_id_ano,
    fk_id_nr_zona,
    nr_cep,
    nr_latitude,
    nr_longitude
)
SELECT distinct
    da.id AS fk_id_ano,
    dze.id AS fk_id_nr_zona,
    tc2016.nr_cep,
    tc2016.nr_latitude,
    tc2016.nr_longitude
FROM public.tabela_cep_2016 tc2016
INNER JOIN datawarehouse.dim_ano da ON da.ano = tc2016.aa_eleicao
INNER JOIN datawarehouse.dim_zonas_eleitorais dze ON dze.nr_zona = tc2016.nr_zona;

-- criar tabela cep 2012
CREATE TABLE datawarehouse.dim_cep_2012 (
    id SERIAL PRIMARY KEY,
    fk_id_ano INTEGER NOT NULL REFERENCES datawarehouse.dim_ano(id),
    fk_id_nr_zona INTEGER NOT NULL REFERENCES datawarehouse.dim_zonas_eleitorais(id),
    nr_cep CHAR(8) NOT NULL,
    nr_latitude NUMERIC,
    nr_longitude NUMERIC
);


-- Inserir os dados de 2012
INSERT INTO datawarehouse.dim_cep_2012 (
    fk_id_ano,
    fk_id_nr_zona,
    nr_cep,
    nr_latitude,
    nr_longitude
)
SELECT distinct
    da.id AS fk_id_ano,
    dze.id AS fk_id_nr_zona,
    tc2012.nr_cep,
    tc2012.nr_latitude,
    tc2012.nr_longitude
FROM public.tabela_cep_2012 tc2012
INNER JOIN datawarehouse.dim_ano da ON da.ano = tc2012.aa_eleicao
INNER JOIN datawarehouse.dim_zonas_eleitorais dze ON dze.nr_zona = tc2012.nr_zona;

select count (id) from datawarehouse.dim_cep_2012
select count (id) from datawarehouse.dim_cep_2016
select count (id) from datawarehouse.dim_cep_2020

select * from datawarehouse.dim_zonas_eleitorais
where municipio = 'sao paulo'
order by  estado, municipio, nr_zona


-- criar dimensão ano
CREATE TABLE datawarehouse.dim_ano (
    id SERIAL PRIMARY KEY,
    ano INTEGER NOT NULL
);
insert into datawarehouse.dim_ano(ano)
select distinct ano
from datawarehouse.dim_data_eleicao

ALTER TABLE datawarehouse.dim_data_eleicao
ADD COLUMN fk_id_dim_ano INTEGER REFERENCES datawarehouse.dim_ano(id);

UPDATE datawarehouse.dim_data_eleicao dde
SET fk_id_dim_ano = da.id
FROM datawarehouse.dim_ano da
WHERE dde.ano = da.ano;

ALTER TABLE datawarehouse.dim_data_eleicao
DROP COLUMN ano;

-- criar dimensão mes
CREATE TABLE datawarehouse.dim_mes (
    id SERIAL PRIMARY KEY,
    mes INTEGER NOT NULL
);
insert into datawarehouse.dim_mes(mes)
select distinct mes
from datawarehouse.dim_data_eleicao

ALTER TABLE datawarehouse.dim_data_eleicao
ADD COLUMN fk_id_dim_mes INTEGER REFERENCES datawarehouse.dim_mes(id);

UPDATE datawarehouse.dim_data_eleicao dde
SET fk_id_dim_mes = dm.id
FROM datawarehouse.dim_mes dm
WHERE dde.mes = dm.mes;

ALTER TABLE datawarehouse.dim_data_eleicao
DROP COLUMN mes;

29_11_2024
SELECT
    (SELECT COUNT(*) FROM datawarehouse.dim_cep_2012) AS total_tabela1,
    (SELECT COUNT(*) FROM datawarehouse.dim_cep_2016) AS total_tabela2,
    (SELECT COUNT(*) FROM datawarehouse.dim_cep_2016) AS total_tabela3;

ALTER TABLE datawarehouse.dim_cep_2012
 ALTER COLUMN nr_cep TYPE VARCHAR(9);
 ALTER TABLE datawarehouse.dim_cep_2016
 ALTER COLUMN nr_cep TYPE VARCHAR(9);

ALTER TABLE datawarehouse.dim_cep_2020
ALTER COLUMN nr_cep TYPE VARCHAR(9);
 
create table datawarehouse.dim_bairro
 (id serial primary key,
 nm_bairro varchar (70))
 
INSERT INTO datawarehouse.dim_bairro (nm_bairro)
SELECT DISTINCT nm_bairro
FROM datawarehouse.dim_bairro_comparado;

select * FROM public.tabela_cep_2020
WHERE nm_bairro is null

ALTER TABLE datawarehouse.dim_bairro_comparado
ADD COLUMN fk_id_municipio INTEGER REFERENCES datawarehouse.dim_municipio(id);

insert into datawarehouse.dim_bairro_comparado (fk_id_municipio)
select
	dm.id
from datawarehouse.dim_municipio dm
inner join datawarehouse.dim_bairro_comparado as dbc on dbc.nr_cep = dm.cep 

create table datawarehouse.dim_cep_lt_lg
(id serial primary key,
cep char (9),
latitude numeric,
longitue numeric);
CREATE TABLE datawarehouse.dim_cep_lt_lg (
    id SERIAL PRIMARY KEY,
    cep CHAR(9),
    latitude NUMERIC,
    longitude NUMERIC
);

INSERT INTO datawarehouse.dim_cep_lt_lg (cep, latitude, longitude)
SELECT DISTINCT nr_cep, nr_latitude, nr_longitude
FROM datawarehouse.dim_cep_2012
UNION
SELECT DISTINCT nr_cep, nr_latitude, nr_longitude
FROM datawarehouse.dim_cep_2016
UNION
SELECT DISTINCT nr_cep, nr_latitude, nr_longitude
FROM datawarehouse.dim_cep_2020;

ALTER TABLE datawarehouse.dim_bairro_comparado
ADD COLUMN fk_id_cep_lt_lg INT;

ALTER TABLE datawarehouse.dim_bairro_comparado
ADD CONSTRAINT fk_bairro_cep
FOREIGN KEY (fk_id_cep_lt_lg) REFERENCES datawarehouse.dim_cep_lt_lg(id);

-- Passo 2: Atualizar a chave estrangeira com base em nr_cep, nr_latitude e nr_longitude
UPDATE datawarehouse.dim_bairro_comparado dbc
SET fk_id_cep_lt_lg = dcl.id
FROM datawarehouse.dim_cep_lt_lg dcl
WHERE dbc.nr_cep = dcl.cep
  AND dbc.nr_latitude = dcl.latitude
  AND dbc.nr_longitude = dcl.longitude;

30_11_2024
ALTER TABLE datawarehouse.dim_bairro_comparado
ADD COLUMN fk_id_municipio INTEGER,
ADD CONSTRAINT fk_id_municipio
    FOREIGN KEY (fk_id_municipio) 
    REFERENCES datawarehouse.dim_municipio(id);

UPDATE datawarehouse.dim_bairro_comparado dbc
SET fk_id_municipio = dm.id
FROM datawarehouse.dim_municipio dm
WHERE  dm.nm_municipio =dbc.nm_municipio;

UPDATE datawarehouse.dim_bairro_comparado 
SET nm_municipio = lower(replace(unaccent(nm_municipio), 'ç', 'c')) 
WHERE nm_municipio is not null 

UPDATE datawarehouse.dim_municipio
SET nm_municipio = LOWER(REPLACE(UNACCENT(REPLACE(nm_municipio, 'ç', 'c')), '''', ''))
WHERE nm_municipio IS NOT NULL;

select distinct nm_municipio from datawarehouse.dim_bairro_comparado
where fk_id_municipio is null

ALTER TABLE datawarehouse.dim_bairro_comparado 
DROP COLUMN nm_municipio;

ALTER TABLE datawarehouse.dim_bairro_comparado 
DROP COLUMN cd_municipio;

UPDATE datawarehouse.dim_bairro_comparado dbc
SET fk_id_sg_uf = dsu.id
FROM datawarehouse.dim_sg_uf dsu
WHERE dbc.fk_id_sg_uf = dsu.sg_uf;

select fk_id_sg_uf from datawarehouse.dim_bairro_comparado
where fk_id_sg_uf is null

ALTER TABLE datawarehouse.dim_bairro_comparado
ADD COLUMN fk_id_cep INTEGER,
ADD CONSTRAINT fk_id_cep
    FOREIGN KEY (fk_id_cep) 
    REFERENCES datawarehouse.dim_cep_lt_lg(id);

UPDATE datawarehouse.dim_bairro_comparado dbc
SET fk_id_cep = ddc.id
FROM datawarehouse.dim_cep_lt_lg ddc
WHERE ddc.cep = dbc.nr_cep
and ddc.latitude = dbc.nr_latitude
and ddc.longitude = dbc.nr_longitude

select distinct nr_cep, nr_latitude, nr_longitude from datawarehouse.dim_bairro_comparado
where nr_cep = '0.0'

select distinct cep, latitude, longitude from datawarehouse.dim_cep_lt_lg
where cep = '0'

select distinct cep  from datawarehouse.dim_cep_lt_lg
where cep = '0'

update datawarehouse.dim_bairro_comparado
set nr_cep = '0'
where nr_cep = '0.0'

ALTER TABLE datawarehouse.dim_bairro_comparado 
DROP COLUMN nr_cep;

ALTER TABLE datawarehouse.dim_bairro_comparado 
DROP COLUMN nr_latitude;

ALTER TABLE datawarehouse.dim_bairro_comparado 
DROP COLUMN nr_longitude;

ALTER TABLE datawarehouse.dim_bairro_comparado 
DROP COLUMN bairro_normalizado;

ALTER TABLE datawarehouse.dim_bairro
ALTER COLUMN fk_id_sg_uf
TYPE INTEGER USING fk_id_sg_uf::INTEGER;

create table datawarehouse.fato_eleicoes
(id serial primary key,
fk_id_ano integer REFERENCES datawarehouse.dim_ano(id) ,
fk_id_bairro integer REFERENCES datawarehouse.dim_bairro(id) ,
fk_id_cep integer REFERENCES datawarehouse.dim_cep_lt_lg(id),
fk_data_eleicao integer REFERENCES datawarehouse.dim_data_eleicao(id),
fk_ds_eleicao integer REFERENCES datawarehouse.dim_ds_eleicao(id),
fk_id_legenda integer REFERENCES datawarehouse.dim_legenda(id),
fk_id_municipio integer REFERENCES datawarehouse.dim_municipio(id) ,
fk_ik_nome_candidato integer REFERENCES datawarehouse.dim_nome_candidato(id),
fk_id_turno integer REFERENCES datawarehouse.dim_turno(id),
fk_id_zona integer REFERENCES datawarehouse.dim_zonas_eleitorais(id),
qtd_votos integer,
qtd_abstencoes integer,
qtd_aptos integer
)

select distinct
da.id ,
dde.id,
dl.id ,
dm.id ,
dsu.id,
dze.id,
from public.eleicoes pe
inner join  datawarehouse.dim_ano as da on da.ano = pe.aa_eleicao
inner join datawarehouse.dim_data_eleicao as dde on dde.fk_id_ano = da.id
inner join datawarehouse.dim_legenda as dl on dl.nr_votavel = pe.nr_votavel
inner join datawarehouse.dim_municipio as dm on dm.cd_municipio = pe.cd_municipio
inner join datawarehouse.dim_sg_uf as dsu on dsu.sg_uf = dsu.sg_uf
inner join datawarehouse.dim_zonas_eleitorais as dze on dze.nr_zona = pe.nr_zona

dt.id 
inner join datawarehouse.dim_turno as dt on dt.nr_turno = pe.nr_turno
dnc.id,
inner join datawarehouse.dim_nome_candidato as dnc on dnc.nm_votavel = pe.nm_votavel
de.id
inner join datawarehouse.dim_ds_eleicao as de on de.ds_eleicao = pe.ds_eleicao




dib.id,
dcep.id,
pe.qt_votos,
pe.qt_abstencoes,
pe.qt_aptos



inner join datawarehouse.dim_bairro as dib on dib.fk_id_municipio = dm.id 
inner join datawarehouse.dim_cep_lt_lg as dcep on dcep.id = dib.fk_id_cep

 








30_11_2024
select distinct
da.id,
dbr.id,
dcep.id,
dt.id,
dl.id,
dze.id,
pe.qt_votos,
pe.qt_abstencoes,
pe.qt_aptos
from datawarehouse.dim_bairro dbr
inner join datawarehouse.dim_ano as da on da.id = dbr.fk_id_ano
inner join datawarehouse.dim_cep_lt_lg as dcep on dcep.id = dbr.fk_id_cep
inner join public.eleicoes as pe on pe.aa_eleicao = da.ano
inner join datawarehouse.dim_turno as dt on dt.nr_turno = pe.nr_turno
inner join datawarehouse.dim_legenda as dl on dl.nr_votavel = pe.nr_votavel
inner join datawarehouse.dim_zonas_eleitorais as dze on dze.fk_id_municipio = dbr.fk_id_municipio


create table datawarehouse.fato_eleicoes_02
(id serial PRIMARY key,
ano_eleicao integer,
uf char (2),
cd_municipio integer,
nr_zona integer,
nr_votavel bigint,
qt_votos integer,
qt_abstencoes integer,
qt_aptos integer)


insert into datawarehouse.fato_eleicoes_02 (ano_eleicao,
uf, cd_municipio, nr_zona, nr_votavel, qt_votos ,qt_abstencoes, qt_aptos)
select DISTINCT 
aa_eleicao,
sg_uf,
cd_municipio,
nr_zona,
nr_votavel,
qt_votos,
qt_abstencoes,
qt_aptos
from public.eleicoes

alter table datawarehouse.fato_eleicoes_02
add column fk_id_cep integer

update datawarehouse.fato_eleicoes_02 fe
set uf = dsu.id
from datawarehouse.dim_sg_uf dsu
where dsu.sg_uf = fe.uf

ALTER TABLE datawarehouse.fato_eleicoes_02
ALTER COLUMN fk_id_uf TYPE integer
USING fk_id_uf::integer;

update datawarehouse.fato_eleicoes_02 fe
set fk_id_zona = dze.id
from datawarehouse.dim_zonas_eleitorais dze
where dze.nr_zona = fe.fk_id_zona

update datawarehouse.fato_eleicoes_02 fe
set fk_id_municipio = dm.id
from datawarehouse.dim_municipio dm
where fe.fk_id_municipio = dm.cd_municipio

update datawarehouse.fato_eleicoes_02 fe
set fk_dim_legenda = dl.id
from datawarehouse.dim_legenda dl
where fe.fk_dim_legenda = dl.nr_votavel

--preenchar a fato e a dim_zona
datawarehouse.fato_eleicoes_02
id
fk_dim_ano
fk_id_uf
fk_id_municipio
fk_id_zona
fk_id_cep (esta vazia precisa popular com a chave de cep)
datawarehouse.dim_zonas_eleitorais
id
fk_id_uf
fk_id_municipio
nr_zona
fk_id_bairro((esta vazia precisa popular com a chave de bairro)
fk_id_ano((esta vazia precisa popular com a chave de ano)
datawarehouse.dim_bairro
fk_id_ano
fk_id_sg_uf
fk_id_municipio
fk_id_cep
datawarehouse.dim_ano
id
ano
datawarehouse.dim_cep_lt_lg
id
cep
latitude
longitude
datawarehouse.dim_data_eleicao
id
dia
fk_id_ano
fk_id_mes
datawarehouse.dim_legenda
id
nr_votavel
datawarehouse.dim_municipio
id
cd_municipio
nm_municipio
fk_id_sg_uf
datawarehouse.dim_nome_candidato
id
nm_votavel
legenda
fk_id_dim_legenda
datawarehouse.dim_sg_uf
id
sg_uf
datawarehouse.dim_turno
id
nr_turno

UPDATE datawarehouse.fato_eleicoes_02 fato
SET fk_id_cep = (
    SELECT cep.id
    FROM datawarehouse.dim_cep_lt_lg cep
    JOIN datawarehouse.dim_municipio mun ON fato.fk_id_municipio = mun.id
    WHERE cep.cep = (
        SELECT dim_cep_lt_lg.cep
        FROM datawarehouse.dim_cep_lt_lg
        WHERE dim_cep_lt_lg.id = fato.fk_id_cep
    )
);

UPDATE datawarehouse.dim_zonas_eleitorais zona
SET fk_id_bairro = (
    SELECT bairro.id
    FROM datawarehouse.dim_bairro bairro
    WHERE bairro.fk_id_municipio = zona.fk_id_municipio
    AND bairro.fk_id_sg_uf = zona.fk_id_uf
);

UPDATE datawarehouse.dim_zonas_eleitorais zona
SET fk_id_ano = (
    SELECT ano.id
    FROM datawarehouse.dim_ano ano
    WHERE ano.ano = EXTRACT(YEAR FROM CURRENT_DATE) -- Substitua pelo ano desejado
);

02/12/2024

--ALTERAÇÕES NA FATO ELEIÇÃO
--sg_uf vira fk_sg_uf
UPDATE datawarehouse.fato_eleicao fe
SET sg_uf = dsu.id
FROM datawarehouse.dim_sg_uf dsu
WHERE dsu.sg_uf = fe.sg_uf

--alterar de char para integer
ALTER TABLE datawarehouse.fato_eleicao
ALTER COLUMN sg_uf TYPE integer
USING CAST(sg_uf AS integer);

--aa_eleicao vira fk_dim_ano
UPDATE datawarehouse.fato_eleicao fe
SET aa_eleicao = da.id
FROM datawarehouse.dim_ano da
WHERE da.ano = fe.aa_eleicao

--cd_municipio vira fk_dim_municipio
UPDATE datawarehouse.fato_eleicao fe
SET cd_municipio = dm.id
FROM datawarehouse.dim_municipio dm
WHERE dm.cd_municipio = fe.cd_municipio

--nr_zona vira fk_dim_zona
UPDATE datawarehouse.fato_eleicao fe
SET nr_zona = dze.id
FROM datawarehouse.dim_zonas_eleitorais dze
WHERE dze.nr_zona = fe.nr_zona

--falta a partir daqui !!!!!!!

--nr_turno vira fk_dim_turno
UPDATE datawarehouse.fato_eleicao fe
SET nr_turno = dt.id
FROM datawarehouse.dim_turno dt
WHERE dt.nr_turno = fe.nr_turno

--nr_votavel vira fk_dim_legenda
UPDATE datawarehouse.fato_eleicao fe
SET nr_votavel = dl.id
FROM datawarehouse.dim_legenda dl
WHERE dl.nr_votavel = fe.nr_votavel

select distinct count(fk_dim_id_cep) from datawarehouse.fato_cep_python

select distinct count(id) from datawarehouse.dim_cep_lt_lg

select distinct count(fk_id_cep) from datawarehouse.dim_bairro

04_12_2024
update datawarehouse.fato_cep fcep
set nr_cep = dcep.id
from datawarehouse.dim_cep dcep
where fcep.nr_cep = dcep.nr_cep

select bairro_normalizado from datawarehouse.dim_bairro
where bairro_normalizado is NULL

select bairro_normalizado from datawarehouse.fato_cep
where bairro_normalizado is NULL

select nr_cep from datawarehouse.fato_cep
where nr_cep is NULL

select DISTINCT (cd_municipio) from datawarehouse.dim_municipio
select DISTINCT (cd_municipio) from datawarehouse.fato_cep

select DISTINCT (nr_cep) from datawarehouse.dim_cep
select DISTINCT (nr_cep) from datawarehouse.fato_cep

select distinct 
nr_zona,
cd_municipio,
nm_municipio
from datawarehouse.fato_eleicao fe
inner join datawarehouse.fato_cep as fcep on fcep.nr_zona 

CREATE TABLE IF NOT EXISTS datawaredim_zona (
    id SERIAL PRIMARY KEY,
    sg_uf CHAR(2),
    cd_municipio INTEGER,
    nr_zona BIGINT,
    UNIQUE (sg_uf, cd_municipio, nr_zona)
);

-- Etapa 2: Inserir dados únicos das tabelas fato_cep e fato_eleicao
INSERT INTO datawarehouse.dim_zona (sg_uf, cd_municipio, nr_zona)
SELECT DISTINCT sg_uf, cd_municipio, nr_zona
FROM (
    SELECT sg_uf, cd_municipio, nr_zona FROM datawarehouse.fato_cep
    UNION
    SELECT sg_uf, cd_municipio, nr_zona FROM datawarehouse.fato_eleicao
) AS zonas_unificadas;

adiconar em dim_municipio

INSERT INTO datawarehouse.dim_municipio (sg_uf, cd_municipio,nm_municipio )
SELECT DISTINCT sg_uf, cd_municipio, nm_municipio
FROM (
    SELECT sg_uf, cd_municipio, nm_municipio FROM datawarehouse.fato_cep
    UNION
    SELECT sg_uf, cd_municipio FROM datawarehouse.fato_eleicao
) AS zonas_unificadas;




update datawarehouse.dim_zona dz
set nr_cep = fcep.nr_cep 
from datawarehouse.fato_cep fcep
where dz.sg_uf = fcep.sg_uf
and dz.cd_municipio = fcep.cd_municipio
and dz.nr_zona = fcep.nr_zona

--verifica os nulos e estão aparecendo os do ano de 2008
-- existem 136 zonas que não achei o cep
select * from datawarehouse.dim_zona
where nr_cep is null
order by cd_municipio

select * from datawarehouse.fato_cep
where nr_cep ilike '0'

alter table datawarehouse.dim_zona
add column nm_bairro text

update datawarehouse.dim_zona dz
set nm_bairro = fe.nm_bairro
from datawarehouse.fato_cep fe
where fe.nr_cep = dz.nr_cep

update datawarehouse.dim_zona dz
set nm_bairro = fe.nm_bairro
from datawarehouse.fato_cep fe
where fe.nr_cep = dz.nr_cep

select nm_bairro,nr_cep from datawarehouse.fato_cep
where nr_zona = 340
and cd_municipio = 46132

SELECT nr_zona,cd_municipio,sg_uf,aa_eleicao from datawarehouse.fato_eleicao
where nr_zona = 340
and cd_municipio = 46132

select * from datawarehouse.dim_zona

SELECT cd_municipio from datawarehouse.dim_municipio


--colocando as chaves estrangeiras na fato_cep
update datawarehouse.dim_zona dz
set sg_uf = dsu.id
from datawarehouse.dim_sg_uf dsu
where dz.sg_uf = dsu.sg_uf

ALTER TABLE datawarehouse.dim_zona
ALTER COLUMN fk_dim_sg_uf TYPE integer
USING fk_dim_sg_uf::integer;

update datawarehouse.dim_zona dz
set fk_dim_sg_uf = dm.id
from datawarehouse.dim_municipio dm
where dz.fk_dim_sg_uf = dm.cd_municipio

12818 1074	"CRUZEIRO DO SUL"	"AC"
select id, cd_municipio, nm_municipio, sg_uf from datawarehouse.dim_municipio 
order by sg_uf

select fk_dim_sg_uf fk_dim_municipio from datawarehouse.dim_municipio 


update datawarehouse.dim_zona dz
set nr_cep = dcep.id
from datawarehouse.dim_cep dcep
where dz.nr_cep = dcep.nr_cep

update datawarehouse.dim_zona dz
set fk_dim_cep = '0'
where fk_dim_cep = '0.0'

update datawarehouse.dim_zona dz
set fk_dim_cep = '88587'
where fk_dim_cep = '88587.0'

ALTER TABLE datawarehouse.dim_zona
ALTER COLUMN fk_dim_cep TYPE integer
USING fk_dim_cep::integer;

update datawarehouse.dim_zona dz
set bairro_normalizado = dbr.id
from datawarehouse.dim_bairro dbr
where dz.bairro_normalizado = dbr.bairro_normalizado

alter table datawarehouse.dim_zona
add column bairro_normalizado text

update datawarehouse.dim_zona dz
set bairro_normalizado = fcep.bairro_normalizado
from datawarehouse.fato_cep fcep
where fcep.nm_bairro = dz.nm_bairro

ALTER TABLE datawarehouse.dim_zona
ALTER COLUMN bairro_normalizado TYPE integer
USING bairro_normalizado::integer;

update datawarehouse.fato_cep fcep
set bairro_normalizado = dbr.id
from datawarehouse.dim_bairro dbr
where dbr.bairro_normalizado = fcep.bairro_normalizado

ALTER TABLE datawarehouse.fato_cep
ALTER COLUMN fk_dim_bairro TYPE integer
USING fk_dim_bairro::integer;

update datawarehouse.fato_cep fcep
set nr_cep = dcep.id
from datawarehouse.dim_cep dcep
where dcep.nr_cep = fcep.nr_cep

ALTER TABLE datawarehouse.fato_cep
ALTER COLUMN  nr_cep TYPE integer
USING nr_cep::integer;

update datawarehouse.fato_cep fcep
set nr_zona = dz.id
from datawarehouse.dim_zona dz
where dz.nr_zona = fcep.nr_zona

INSERT INTO datawarehouse.dim_municipio (cd_municipio, nm_municipio, sg_uf)
SELECT DISTINCT fcep.cd_municipio, fcep.nm_municipio, fcep.sg_uf
FROM datawarehouse.fato_cep fcep;


select distinct cd_municipio,nm_municipio from datawarehouse.fato_cep
order by cd_municipio
--5616 resultados

select distinct cd_municipio,nm_municipio from public.eleicoes
order by cd_municipio
--5602 

select distinct cd_municipio,nm_municipio from public.tabela_cep_2012
order by cd_municipio
--5568

select distinct cd_municipio,nm_municipio from public.tabela_cep_2016
order by cd_municipio
--5568

select distinct cd_municipio,nm_municipio from public.tabela_cep_2020
order by cd_municipio
--5568

update datawarehouse.fato_cep fcep
set cd_municipio = dm.id
from datawarehouse.dim_municipio dm
where dm.cd_municipio = fcep.cd_municipio

select 
dm.nm_municipio,
fcep.fk_dim_municipio
from datawarehouse.fato_cep fcep
inner join datawarehouse.dim_municipio as dm on dm.id = fcep.fk_dim_municipio

select id, nm_municipio from datawarehouse.dim_municipio
where  nm_municipio ilike 'teresina'

update datawarehouse.fato_cep fcep
set sg_uf = dsg.id
from datawarehouse.dim_sg_uf dsg
where dsg.sg_uf = fcep.sg_uf

ALTER TABLE datawarehouse.fato_cep
ALTER COLUMN fk_dim_sg_uf  TYPE integer
USING fk_dim_sg_uf::integer;


--colocando as chaves estrangeiras na fato_eleicao

update datawarehouse.fato_eleicao fe
set aa_eleicao = da.id
from datawarehouse.dim_ano da
where da.ano = fe.aa_eleicao

update datawarehouse.fato_eleicao fe
set sg_uf = dsg.id
from datawarehouse.dim_sg_uf dsg
where dsg.sg_uf = fe.sg_uf

ALTER TABLE datawarehouse.fato_eleicao
ALTER COLUMN  sg_uf TYPE integer
USING sg_uf ::integer;

update datawarehouse.fato_eleicao fe
set cd_municipio = dm.id
from datawarehouse.dim_municipio dm
where dm.cd_municipio = fe.cd_municipio

update datawarehouse.fato_eleicao fe
set nr_zona = dz.id
from datawarehouse.dim_zona dz
where dz.nr_zona = fe.nr_zona

update datawarehouse.fato_eleicao fe
set nr_turno = dt.id
from datawarehouse.dim_turno dt
where dt.nr_turno = fe.nr_turno

update datawarehouse.fato_eleicao fe
set nr_votavel = dl.id
from datawarehouse.dim_legenda dl
where dl.nr_votavel = fe.nr_votavel

05/12/2024

SELECT 
    nr_latitude AS latitude,
    nr_longitude AS longitude
FROM 
    datawarehouse.fato_cep
	limit 30


UPDATE sua_tabela
SET latitude = NULL, longitude = NULL
WHERE latitude = -1.0 AND longitude = -1.0;

ALTER TABLE datawarehouse.fato_cep
ALTER COLUMN nr_latitude TYPE NUMERIC(10,6),
ALTER COLUMN nr_longitude TYPE NUMERIC(10,6);

ALTER TABLE datawarehouse.dim_cep
ALTER COLUMN nr_latitude TYPE NUMERIC(10,6),
ALTER COLUMN nr_longitude TYPE NUMERIC(10,6);


ALTER TABLE datawarehouse.fato_cep
ALTER COLUMN nr_latitude TYPE NUMERIC(10,6),
ALTER COLUMN nr_longitude TYPE NUMERIC(10,6);

ALTER TABLE datawarehouse.dim_cep
ALTER COLUMN nr_latitude TYPE NUMERIC(10,6),
ALTER COLUMN nr_longitude TYPE NUMERIC(10,6);

select distinct
da.ano,
dsu.sg_uf,
dz.nr_zona,
dl.nr_votavel,
dbr.bairro_normalizado,
fe.qt_votos
from datawarehouse.fato_eleicao fe 
inner join datawarehouse.dim_ano as da on da.id = fe.fk_dim_ano
inner join datawarehouse.fato_cep as fcep on fcep.fk_dim_zona = fe.fk_dim_zona
inner join datawarehouse.dim_sg_uf as dsu on dsu.id = fe.fk_dim_sg_uf
inner join datawarehouse.dim_bairro as dbr on dbr.id = fcep.fk_dim_bairro
INNER join datawarehouse.dim_zona as dz on dz.fk_dim_sg_uf = dsu.id
inner join datawarehouse.dim_legenda as dl on dl.id = fe.fk_nr_votavel

select distinct sum(qt_votos) from public.eleicoes
WHERE sg_uf = 'CE'
and aa_eleicao = '2012'


select distinct
dsu.sg_uf,
da.ano,
sum(qt_votos) as soma
from datawarehouse.fato_eleicao fe
inner join datawarehouse.dim_sg_uf as dsu on dsu.id = fe.fk_dim_sg_uf
inner join datawarehouse.dim_ano as da on da.id = fe.fk_dim_ano
WHERE dsu.sg_uf = 'CE'
and da.ano = '2012'
group by dsu.sg_uf,da.ano;






o quanto os partidos de direita / esquerda creceram ao longo das ultimas 4 eleições 
	-delimitar por por municipio
	-delimitar por estado
	- delimitar por região

(GABRIEL) 2 historico de 1 e 2 turnos 
	- quais foram os partidos que se elegeram em primeiro turno.(partidos fortes)

(JONATHAN)3  Quanto o fundo eleitoral beneficiou os partidos
	- vincular a tabela externa (achar a tabela de valores)
	- quanto cada candidato eleito gastou em media
		- delimitar por ano
		- delimitar por partido

(JOSIANE) 4 aumento / diminuição de votos brancos e nulos 
	-  delimitar por por municipio
	-  delimitar por estado
	-  delimitar por região

07/12/2024


CREATE TABLE datawarehouse.cep_denovo(
	id serial primary key,
    nr_cep integer,
    nr_latitude numeric,
    nr_longitude numeric,
	nr_zona integer,
	ds_endereco CHARACTER VARYING(100),
	cd_municipio BIGINT,
	sg_uf CHARACTER VARYING (30),
    aa_eleicao BIGINT,
	nm_bairro CHARACTER VARYING (100),
    nm_municipio CHARACTER VARYING (100),
    fk_id_ano INTEGER
);



INSERT INTO datawarehouse.cep_denovo (nr_cep, nr_latitude, nr_longitude,ds_endereco, nr_zona,cd_municipio,sg_uf,
    aa_eleicao, nm_bairro, nm_municipio, fk_id_ano)
SELECT DISTINCT
        t.nr_cep,
        t.nr_latitude,
        t.nr_longitude,
		unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.ds_endereco, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
		t.nr_zona,  -- Casting explícito para INTEGER
        t.cd_municipio,  -- Casting explícito para INTEGER
        t.sg_uf,
        t.aa_eleicao,
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_bairro, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_municipio, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
		da.id AS fk_id_ano
FROM public.tabela_cep_2012 t
INNER JOIN datawarehouse.dim_ano da ON da.ano = t.aa_eleicao
UNION ALL
SELECT DISTINCT
        t.nr_cep,
        t.nr_latitude,
        t.nr_longitude,
		unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.ds_endereco, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
		t.nr_zona,  -- Casting explícito para INTEGER
        t.cd_municipio,  -- Casting explícito para INTEGER
        t.sg_uf,
        t.aa_eleicao,
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_bairro, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_municipio, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
		da.id AS fk_id_ano
FROM public.tabela_cep_2016 t
INNER JOIN datawarehouse.dim_ano da ON da.ano = t.aa_eleicao
UNION ALL
SELECT DISTINCT
        t.nr_cep,
        t.nr_latitude,
        t.nr_longitude,
		unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.ds_endereco, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
		t.nr_zona,  -- Casting explícito para INTEGER
        t.cd_municipio,  -- Casting explícito para INTEGER
        t.sg_uf,
        t.aa_eleicao,
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_bairro, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
        unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(t.nm_municipio, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))),
		da.id AS fk_id_ano
FROM public.tabela_cep_2020 t
INNER JOIN datawarehouse.dim_ano da ON da.ano = t.aa_eleicao


-- create table datawarehouse.dim_bairro
-- (id serial primary key,
-- nm_bairro TEXT,
-- fk_id_cd_municipio bigint,
-- fk_id_sg_uf CHARACTER VARYING (2),
-- fk_id_cep integer)

-- insert into datawarehouse.dim_bairro(nm_bairro, fk_id_cd_municipio,fk_id_sg_uf,fk_id_cep)
-- select distinct
-- nm_bairro,
-- cd_municipio,
-- sg_uf,
-- nr_cep 
-- from datawarehouse.cep_denovo

-- ALTER TABLE public.tabela_cep_2012
-- ALTER COLUMN nm_municipio TYPE CHARACTER VARYING(100);

-- update datawarehouse.dim_bairro
-- set fk_id_sg_uf = dsu.id
-- from datawarehouse.dim_sg_uf dsu
-- where dsu.sg_uf = fk_id_sg_uf

-- CREATE TABLE datawarehouse.dim_cep(
-- 	id serial primary key,
--     nr_cep integer,
--     nr_latitude numeric,
--     nr_longitude numeric );

-- insert into datawarehouse.dim_cep(nr_cep,nr_latitude,nr_longitude)
-- select distinct
-- nr_cep,
-- nr_latitude,
-- nr_longitude
-- from datawarehouse.cep_denovo

update datawarehouse.dim_bairro
set fk_id_cep = dcep.id
from datawarehouse.dim_cep dcep
where nr_cep = fk_id_cep

CREATE TABLE datawarehouse.dim_municipio(
	id serial primary key,
	cd_municipio bigint,
	sg_uf CHARACTER VARYING (2),
    nm_municipio CHARACTER VARYING (100)
);
insert into datawarehouse.dim_municipio(cd_municipio, sg_uf, nm_municipio)
select distinct
cd_municipio,
sg_uf,
nm_municipio 
from datawarehouse.cep_denovo

update datawarehouse.dim_bairro
set fk_id_cd_municipio = dm.id
from datawarehouse.dim_municipio dm
where dm.cd_municipio = fk_id_cd_municipio

update datawarehouse.dim_municipio dm
set sg_uf = dsu.id
from  datawarehouse.dim_sg_uf dsu
where dsu.sg_uf = dm.sg_uf

ALTER TABLE datawarehouse.dim_municipio
ALTER COLUMN fk_id_sg_uf TYPE integer USING fk_id_sg_uf::integer;

ALTER TABLE datawarehouse.dim_bairro
ALTER COLUMN fk_id_sg_uf TYPE integer USING fk_id_sg_uf::integer;

insert into datawarehouse.dim_zona(fk_dim_sg_uf, fk_dim_bairro, fk_dim_municipio, nr_zona,fk_dim_cep)
select distinct
dsu.id as fk_dim_sg_uf,
dbr.id as fk_dim_bairro,
dm.id as fk_dim_municipio,
fcepnovo.nr_zona as nr_zona,
dcep.id as fk_dim_cep
from datawarehouse.cep_denovo fcepnovo
inner join datawarehouse.dim_sg_uf as dsu on dsu.sg_uf = fcepnovo.sg_uf
inner join datawarehouse.dim_municipio as dm on dm.nm_municipio = fcepnovo.nm_municipio
inner join datawarehouse.dim_cep as dcep on dcep.nr_cep = fcepnovo.nr_cep
inner join datawarehouse.dim_bairro as dbr on dbr.nm_bairro = fcepnovo.nm_bairro

–faltam as tabelas fato, 



09/12/2024
hoje:
SELECT * FROM public.eleicoes
LIMIT 100

SELECT * FROM datawarehouse.fato_eleicao
ORDER BY id ASC LIMIT 100

SELECT distinct
dcep.nr_cep,
dcep.nr_latitude,
dcep.nr_longitude,
dbr.nm_bairro,
dm.nm_municipio,
dsu.sg_uf
FROM datawarehouse.dim_cep dcep
inner join datawarehouse.dim_bairro as dbr on dbr.fk_id_cep = dcep.id
inner join datawarehouse.dim_municipio as dm on dm.id = dbr.fk_id_cd_municipio
inner join datawarehouse.dim_sg_uf as dsu on dsu.id = dm.fk_id_sg_uf
WHERE LENGTH(nr_cep::TEXT) < 8
order by nr_latitude

SELECT distinct nr_cep, nr_latitude,nr_longitude
FROM datawarehouse.dim_cep
WHERE LENGTH(nr_cep::TEXT) < 8
order by nr_latitude

SELECT nr_cep
FROM datawarehouse.dim_cep
WHERE LENGTH(nr_cep::TEXT) < 8;

SELECT distinct (LENGTH(nr_cep::TEXT)<8) AS tamanho
FROM datawarehouse.dim_cep;



alter table datawarehouse.fato_eleicao 
ALTER COLUMN fk_dim_sg_uf TYPE integer USING fk_dim_sg_uf::integer;

update datawarehouse.fato_eleicao
set fk_nr_votavel = dl.id
from  datawarehouse.dim_legenda dl
where  dl.nr_votavel = fk_nr_votavel

update datawarehouse.fato_eleicao
set fk_dim_turno = dt.id
from  datawarehouse.dim_turno dt
where  dt.nr_turno = fk_dim_turno

update datawarehouse.fato_eleicao
set fk_dim_zona = dz.id
from datawarehouse.dim_zona dz
where  dz.nr_zona = fk_dim_zona

update datawarehouse.fato_eleicao
set   fk_dim_sg_uf = dsu.id
from datawarehouse.dim_sg_uf dsu
where  dsu.sg_uf = fk_dim_sg_uf
update datawarehouse.fato_eleicao
set   fk_dim_cd_municipio = dm.id
from datawarehouse.dim_municipio dm
where  dm.fk_id_cd_municipio = fk_dim_cd_municipio


select distinct
ds_endereco,
nm_municipio,
sg_uf,
from public.tabela_cep_2012
WHERE LENGTH(nr_cep::TEXT) < 8

10_12_2024

-- INSERT INTO datawarehouse.endereço_cep(ds_endereco, nm_municipio, sg_uf, nr_cep)
--   SELECT DISTINCT
--             unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(ds_endereco, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))) AS ds_endereco,
--             unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(nm_municipio, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))) AS nm_municipio,
--             sg_uf,
--             nr_cep
--         FROM public.tabela_cep_2012
     
--         UNION
--         SELECT DISTINCT
--             unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(ds_endereco, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))) AS ds_endereco,
--             unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(nm_municipio, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))) AS nm_municipio,
--             sg_uf,
--             nr_cep
--         FROM public.tabela_cep_2016
        
--         UNION
--         SELECT DISTINCT
--             unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(ds_endereco, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))) AS ds_endereco,
--             unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(nm_municipio, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g')))) AS nm_municipio,
--             sg_uf,
--             nr_cep
--         FROM public.tabela_cep_2020

--   CREATE TABLE IF NOT EXISTS datawarehouse.endereço_cep(
--         id SERIAL PRIMARY KEY,
--         ds_endereco CHARACTER VARYING(100),
--         nm_municipio CHARACTER VARYING(100),
--         sg_uf CHARACTER VARYING(2),
--         nr_cep INTEGER
--     );

-- update datawarehouse.endereço_cep ec
-- set nm_municipio = dm.id
-- from datawarehouse.dim_municipio dm
-- where dm.nm_municipio =  ec.nm_municipio

-- update datawarehouse.endereço_cep ec
-- set sg_uf = dsu.id
-- from datawarehouse.dim_sg_uf dsu
-- where dsu.sg_uf =  ec.sg_uf

-- update datawarehouse.endereço_cep ec
-- set nr_cep = dc.id
-- from datawarehouse.dim_cep dc
-- where dc.nr_cep =  ec.nr_cep

-- select  
-- de.ds_endereco,
-- dc.nr_cep,
-- dsu.sg_uf,
-- dm.nm_municipio
-- from datawarehouse.dim_endereco de
-- inner join datawarehouse.dim_cep as dc on dc.id = de.fk_nr_cep
-- inner join datawarehouse.dim_sg_uf as dsu on dsu.id = de.fk_sg_uf
-- inner join datawarehouse.dim_municipio as dm on dm.id = de.fk_nm_municipio
-- WHERE LENGTH(nr_cep::TEXT) < 8


-- alter table datawarehouse."dim_endereço"
-- ALTER COLUMN fk_nm_municipio TYPE integer USING fk_nm_municipio::integer;

-- alter table datawarehouse."dim_endereço"
-- ALTER COLUMN fk_sg_uf TYPE integer USING fk_sg_uf::integer;

-- alter table datawarehouse."dim_endereço"
-- ALTER COLUMN fk_nr_cep TYPE integer USING fk_nr_cep::integer;

select 
ds_endereco,
dm.nm_municipio,
dsu.sg_uf,
dcep.nr_cep
from datawarehouse.dim_endereco as de
inner join datawarehouse.dim_sg_uf as dsu ON dsu.id = de.fk_sg_uf
inner join datawarehouse.dim_cep as dcep on dcep.id = de.fk_nr_cep
inner join datawarehouse.dim_municipio as dm on dm.id = de.fk_nm_municipio
update datawarehouse.cep_denovo
where length(nr_cep::text) < 8

update datawarehouse.dim_endereco
set fk_nr_cep = dcep.nr_cep
from datawarehouse.dim_cep dcep
where fk_nr_cep = dcep.id

select distinct * from datawarehouse.cep_denovo
where length(nr_cep::text) < 8

select distinct * from datawarehouse.cepes_api
where length(nr_cep::text) < 8

UPDATE datawarehouse.cep_denovo cp
SET nr_cep = api.cep_corrigido
FROM datawarehouse.cepes_api api
WHERE LENGTH(cp.nr_cep::text) < 8
  AND cp.ds_endereco = api.ds_endereco
  AND cp.sg_uf = api.sg_uf
  AND cp.nm_municipio = api.nm_municipio;

UPDATE datawarehouse.cepes_api
SET cep_corrigido = REGEXP_REPLACE(cep_corrigido, '-', '', 'g')
WHERE cep_corrigido LIKE '%-%';

alter table datawarehouse.cepes_api
alter COLUMN cep_corrigido TYPE integer USING cep_corrigido::integer


update datawarehouse.cepes_api
set cep_corrigido = 0
where cep_corrigido is null

UPDATE datawarehouse.cepes_api
SET cep_corrigido = REPLACE(cep_corrigido, 'k', '')
WHERE cep_corrigido LIKE '%k%';


select distinct * from datawarehouse.cep_denovo
where nr_cep like '00000000'

UPDATE datawarehouse.cep_denovo
SET nr_cep = CONCAT('0000000', nr_cep)
WHERE LENGTH(nr_cep::text) = 1;

alter table datawarehouse.cep_denovo
alter COLUMN nr_cep TYPE varchar USING nr_cep::varchar

select * from datawarehouse.cepes_api_02
order by cep_corrigido

UPDATE datawarehouse.cepes_api_02
SET cep_corrigido = REGEXP_REPLACE(cep_corrigido, '-', '', 'g')
WHERE cep_corrigido LIKE '%-%';

UPDATE datawarehouse.cepes_api_02
SET cep_corrigido = CONCAT( nr_cep,'00000000')
WHERE LENGTH(cep_corrigido::text) is null ;

UPDATE datawarehouse.cep_denovo cp
SET nr_cep = api2.cep_corrigido
FROM datawarehouse.cepes_api_02 api2
 where cp.ds_endereco = api2.ds_endereco
  AND cp.sg_uf = api2.sg_uf
  AND cp.nm_municipio = api2.nm_municipio
  and cep_corrigido <> '00000000';


select distinct * from datawarehouse.cep_denovo
where nr_cep = '000000000'
where nr_cep is null

update datawarehouse.dim_cep dcep
set nr_cep = dcpnovo.nr_cep 
from datawarehouse.cep_denovo dcpnovo
where dcep.nr_latitude = dcpnovo.nr_latitude
and dcep.nr_longitude = dcpnovo.nr_longitude

select distinct * from datawarehouse.cep_denovo
where nr_cep = '07243100'

update datawarehouse.dim_cep
set nr_cep = 07243100
where nr_cep = 7243100

select distinct * from datawarehouse.dim_cep
where nr_cep = '000000000'

update datawarehouse.dim_cep
set nr_cep = '60000000'
where nr_cep = '000000000'

alter table datawarehouse.dim_cep
alter column nr_cep TYPE varchar USING nr_cep::varchar

select distinct * from datawarehouse.dim_cep
where nr_cep = '60000000'

update  datawarehouse.cep_denovo
set nr_cep = '60000000'
where nr_cep = '000000000'

select distinct * from datawarehouse.cepes_api_04
order by cep_corrigido

UPDATE datawarehouse.cep_denovo cp
SET nr_cep = api4.cep_corrigido
FROM datawarehouse.cepes_api_04 api4
 where cp.ds_endereco = api4.ds_endereco
  AND cp.sg_uf = api4.sg_uf
  AND cp.nm_municipio = api4.nm_municipio
  and cep_corrigido <> '00000000';

update datawarehouse.dim_cep dcep
set nr_cep = dcpnovo.nr_cep 
from datawarehouse.cep_denovo dcpnovo
where dcep.nr_latitude = dcpnovo.nr_latitude
and dcep.nr_longitude = dcpnovo.nr_longitude

select distinct * from datawarehouse.dim_cep
where nr_cep = '60000000'

UPDATE datawarehouse.fato_cep
SET fk_dim_cep = NULL;

UPDATE datawarehouse.dim_zona
SET fk_dim_cep = NULL;

UPDATE datawarehouse.dim_endereco
SET fk_nr_cep = NULL;

UPDATE datawarehouse.dim_bairro
SET fk_id_cep = NULL;

create table datawarehouse.dim_cep
(id serial primary key,
nr_cep varchar(8),
nr_latitude numeric,
nr_longitude numeric);

select nr_cep from datawarehouse.cep_denovo
where length(nr_cep)<8

update datawarehouse.cep_denovo
SET nr_cep = REGEXP_REPLACE(nr_cep, '-', '', 'g')

update datawarehouse.cep_denovo
SET nr_cep = concat(nr_cep, '000')

UPDATE datawarehouse.cep_denovo
SET nr_cep = LEFT(nr_cep, LENGTH(nr_cep) - 3)
where length(nr_cep)>8

insert into datawarehouse.dim_cep
(nr_cep,nr_latitude,nr_longitude)
select distinct nr_cep,nr_latitude,nr_longitude
from datawarehouse.cep_denovo

update datawarehouse.dim_bairro
set fk_id_cep = datawarehouse.dim_cep.id
from datawarehouse.dim_cep
where 
datawarehouse.dim_bairro.fk_id_cd_municipio = datawarehouse.dim_municipio.id
datawarehouse.dim_bairro.fk_id_sg_uf = datawarehouse.dim_sg_uf.id

update datawarehouse.dim_bairro dbr
set fk_id_cep = nr_cep
from datawarehouse.cep_denovo cdn 
where dbr.nm_bairro = cdn.nm_bairro
and dbr.fk_id_sg_uf = cdn.sg_uf
and dbr.fk_id_cd_municipio = (select cdn.cd_municipio from 

select * from datawarehouse.cep_denovo 
where cd_municipio = 3209
and nm_municipio = 'CAMPESTRE DA SERRA'

update datawarehouse.dim_endereco

11_12_2024

select * from datawarehouse.dim_endereco

-- update datawarehouse.cep_denovo
-- set  nr_cep  = 68880000
-- where nr_cep like '000000%'

-- update datawarehouse.cep_denovo cdn
-- set nr_cep = api_04.cep_corrigido
-- from datawarehouse.cepes_api_04 api_04
-- where cdn.sg_uf = 'SP'
-- and api_04.ds_endereco = cdn.ds_endereco
-- and cdn.nm_municipio = api_04.nm_municipio

-- SELECT distinct * FROM datawarehouse.cep_denovo
-- where length (nr_cep)=1 -- aqui foi testado 1,2,3,4,5,6,7
-- order by sg_uf

-- update datawarehouse.cep_denovo
-- set nr_cep = concat('0',nr_cep,'000')
-- where length (nr_cep)= 4

-- update datawarehouse.cep_denovo
-- set nr_cep = concat('0',nr_cep)
-- where length (nr_cep)=7

-- update datawarehouse.cep_denovo
-- set nr_cep = '06814010'
-- where ds_endereco = 'RUA AUGUSTO DE ALMEIDA BATISTA 354'
-- AND length (nr_cep)=6

-- update datawarehouse.cep_denovo cdn
-- set nr_cep = gh.cep_inicio
-- from datawarehouse.tabela_cep_git_hub gh
-- where gh.cidade = cdn.nm_municipio
-- and gh.uf = cdn.sg_uf
-- and length (nr_cep)<8

-- update datawarehouse.cep_denovo cdn
-- set nr_cep = p2020.nr_cep
-- from public.tabela_cep_2020 p2020
-- where cdn.ds_endereco =  unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(p2020.ds_endereco, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g'))))
-- and cdn.sg_uf = p2020.sg_uf

-- SELECT distinct * FROM datawarehouse.cep_denovo
-- where nr_cep like '000%'
-- order by sg_uf

-- SELECT distinct * FROM datawarehouse.cep_denovo
-- where nr_cep like '00%'
-- order by sg_uf

-- update datawarehouse.cep_denovo
-- set  nr_cep  = 68880000
-- where nr_cep like '000000%'

-- update datawarehouse.tabela_cep_git_hub
-- set cidade  = unaccent(Upper(trim(regexp_replace(regexp_replace(regexp_replace(cidade, '\s+', ' ', 'g'), '[-,;.]', ' ', 'g'), '\s+', ' ', 'g'))))

-- SELECT distinct * FROM datawarehouse.cep_denovo
-- where nr_cep is null

-- update datawarehouse.cep_denovo cdn
-- set nr_cep = cgh.cep_inicio
-- from datawarehouse.tabela_cep_git_hub cgh
-- where cdn.nr_cep is null
-- and cgh.uf = cdn.sg_uf
-- and cgh.cidade = cdn.nm_municipio

-- ALTER TABLE datawarehouse.dim_endereco
-- ADD COLUMN id SERIAL PRIMARY KEY;

12_12_2024
UPDATE datawarehouse.dim_cep dcep
set nr_latitude = dca.latitude
from datawarehouse.dim_cep_atualizado dca
where dca.nr_cep = dcep.nr_cep

UPDATE datawarehouse.dim_cep dcep
set nr_longitude = dca.longitude
from datawarehouse.dim_cep_atualizado dca
where dca.nr_cep = dcep.nr_cep

select nr_cep from datawarehouse.cep_denovo
where length(nr_cep)> 8

update datawarehouse.cep_denovo dcep
set nr_cep = regexp_replace(nr_cep, '-','' ,'g')

update datawarehouse.cep_denovo dcep
set nr_cep = concat('0',nr_cep)

--remove a primeira letra
UPDATE  datawarehouse.dim_cep dcep
SET nr_cep = SUBSTRING(nr_cep FROM 2)  -- Remove a primeira letra
WHERE LENGTH(nr_cep) > 8;

update datawarehouse.cep_denovo cdn
set nr_latitude = dca.latitude 
from datawarehouse.dim_cep_atualizado dca
where cdn.nr_cep = dca.nr_cep

update datawarehouse.dim_cep cdn
set nr_latitude = dca.latitude
from datawarehouse.dim_cep_atualizado dca
where cdn.nr_cep = dca.nr_cep

insert into datawarehouse.fato_cep (fk_dim_bairro,fk_dim_cep,fk_id_ano,fk_dim_zona,fk_dim_municipio,fk_dim_sg_uf)
select distinct
nm_bairro,
nr_cep,
aa_eleicao,
nr_zona,
nm_municipio,
sg_uf
FROM datawarehouse.cep_denovo


select distinct ds_local_votacao_endereco from public.eleicoes
where ds_local_votacao_endereco is not null
limit 100

ALTER TABLE datawarehouse.fato_eleicao
ADD COLUMN id SERIAL PRIMARY KEY;

alter table  datawarehouse.fato_eleicao
alter COLUMN qt_comparecimento TYPE integer using qt_comparecimento::integer

insert into datawarehouse.fato_eleicao (fk_dim_ano,fk_dim_sg_uf, fk_dim_cd_municipio,fk_dim_zona,fk_dim_turno,fk_nr_votavel,nm_votavel,
			qt_aptos,qt_comparecimento,qt_abstencoes,qt_votos_nominais,qt_votos)
			select distinct
			aa_eleicao ,
			sg_uf ,
			cd_municipio ,
			nr_zona ,
			nr_turno ,
			nr_votavel ,
			nm_votavel ,
			qt_aptos,
			qt_comparecimento,
			qt_abstencoes,
			qt_votos_nominais,
			qt_votos
			from public.eleicoes

-- update chaves dim MUNICIPIO
-- UPDATE datawarehouse.dim_municipio dm
-- SET fk_sg_uf = dsu.id
-- from datawarehouse.dim_sg_uf dsu
-- where dm.fk_sg_uf = dsu.sg_uf

-- update chaves dim BAIRRO
-- UPDATE  datawarehouse.dim_bairro dbr
-- SET fk_nr_cep = dcep.id
-- from datawarehouse.dim_cep dcep
-- where dcep.nr_cep = dbr.fk_sg_uf


-- UPDATE  datawarehouse.dim_bairro dbr
-- SET fk_cd_municipio = dm.id
-- from datawarehouse.dim_municipio dm
-- where dm.cd_municipio = dsu.sg_uf

-- UPDATE  datawarehouse.dim_bairro dbr
-- SET fk_nr_cep = dcep.id
-- from datawarehouse.dim_cep dcep 
-- where dcep.nr_cep = dbr.fk_nr_cep

-- update chaves dim_ENDERECO

-- UPDATE datawarehouse.dim_endereco
-- SET fk_nr_cep = dcep.id
-- FROM datawarehouse.dim_cep dcep
-- where fk_nr_cep = dcep.nr_cep

-- UPDATE datawarehouse.dim_endereco
-- SET fk_sg_uf = dsu.id
-- FROM datawarehouse.dim_sg_uf dsu
-- where fk_sg_uf = dsu.sg_uf

-- UPDATE datawarehouse.dim_endereco
-- SET fk_nm_bairro = dbr.id
-- FROM datawarehouse.dim_bairro dbr
-- where  fk_nm_bairro = dbr.nm_bairro

-- UPDATE datawarehouse.dim_endereco
-- SET  fk_cd_municipio = dm.id 
-- FROM datawarehouse.dim_municipio dm
-- where  fk_cd_municipio = dm.cd_municipio

-- update chaves dim_zona

-- update datawarehouse.dim_zona
-- set fk_cd_municipio = dm.id
-- from datawarehouse.dim_municipio dm
-- where fk_cd_municipio = dm.cd_municipio

-- update datawarehouse.dim_zona
-- set fk_sg_uf = dsu.id
-- from datawarehouse.dim_sg_uf dsu
-- where  fk_sg_uf = dsu.sg_uf

-- update datawarehouse.dim_zona
-- set fk_nr_cep = dcep.id
-- from datawarehouse.dim_cep dcep
-- where  fk_nr_cep = dcep.nr_cep

-- update datawarehouse.dim_zona
-- set fk_ds_endereco = de.id
-- from datawarehouse.dim_endereco de
-- where  fk_ds_endereco = de.ds_endereco

-- update datawarehouse.dim_zona
-- set fk_nm_bairro = dbr.id
-- from datawarehouse.dim_bairro dbr
-- where fk_nm_bairro = dbr.nm_bairro

--chaves da fato_cep

-- update datawarehouse.fato_cep
-- set fk_dim_cep  = dcep.id
-- from datawarehouse.dim_cep dcep
-- where fk_dim_cep = dcep.nr_cep

-- update datawarehouse.fato_cep
-- set fk_id_ano = da.id
-- from datawarehouse.dim_ano da
-- where fk_id_ano = da.ano

-- update datawarehouse.fato_cep
-- set fk_dim_zona = dz.id
-- from datawarehouse.dim_zona dz
-- where fk_dim_zona = dz.nr_zona

-- update datawarehouse.fato_cep
-- set fk_cd_municipio = dm.id
-- from datawarehouse.dim_municipio dm
-- where fk_cd_municipio = dm.cd_municipio

-- update datawarehouse.fato_cep
-- set  fk_dim_bairro = dbr.id
-- from datawarehouse.dim_bairro dbr
-- where  fk_dim_bairro = dbr.nm_bairro

-- update datawarehouse.fato_cep fcep
-- set  ds_endereco = de.id
-- from datawarehouse.dim_endereco de
-- where fcep.ds_endereco = de.ds_endereco

--chaves da fato_eleicao

-- update datawarehouse.fato_eleicao
-- set fk_dim_ano = da.id
-- from datawarehouse.dim_ano da
-- where fk_dim_ano = da.ano

-- update datawarehouse.fato_eleicao
-- set fk_dim_sg_uf = dsu.id
-- from datawarehouse.dim_sg_uf dsu
-- where fk_dim_sg_uf = dsu.sg_uf

-- update datawarehouse.fato_eleicao
-- set fk_dim_cd_municipio = dm.id
-- from datawarehouse.dim_municipio dm
-- where fk_dim_cd_municipio = dm.cd_municipio

-- update datawarehouse.fato_eleicao
-- set fk_dim_zona = dz.id
-- from datawarehouse.dim_zona dz
-- where fk_dim_zona = dz.nr_zona

-- update datawarehouse.fato_eleicao
-- set fk_dim_turno = dt.id
-- from datawarehouse.dim_turno dt
-- where fk_dim_turno = dt.nr_turno

13/12/2024

-- alter table datawarehouse.dim_bairro
-- alter column fk_nr_cep type integer using  fk_nr_cep::integer

-- select fk_nr_cep from datawarehouse.dim_bairro
-- where fk_nr_cep ilike '0%'

-- --tem cep errrado
-- select fk_nr_cep from datawarehouse.dim_bairro
-- where length(fk_nr_cep)= 7

-- --cep_denovo esta ok
-- select nr_cep from datawarehouse.cep_denovo
-- where length(nr_cep)< 8

-- --cepes_api_01 coreção em sp
-- alter table datawarehouse.cepes_api_01
-- alter column nr_cep type varchar using nr_cep ::varchar

-- update datawarehouse.cepes_api_01
-- set nr_cep = concat('0', nr_cep)
-- where length(nr_cep::text)= 7
-- and sg_uf = 'SP'

-- update datawarehouse.cepes_api_01
-- set nr_cep = concat( nr_cep,'00000')
-- where length(nr_cep::text)= 3

-- update datawarehouse.cepes_api_01
-- set nr_cep = concat( nr_cep,'00000')
-- where length(nr_cep::text)= 3

-- update datawarehouse.cepes_api_01
-- set nr_cep = '74340080'
-- where nr_cep = '502'

-- select nr_cep, sg_uf,ds_endereco from datawarehouse.cepes_api_01
-- where length(nr_cep::text)< 6
-- order by nr_cep desc

-- select nr_cep, sg_uf from datawarehouse.cepes_api_01
-- where length(nr_cep::text) <8
-- order by nr_cep desc

--cepes_api_02 coreção em sp

-- select nr_cep, sg_uf from datawarehouse.cepes_api_02
-- where length(nr_cep::text) <8
-- order by nr_cep desc

-- alter table datawarehouse.cepes_api_02
-- alter column nr_cep type varchar using nr_cep ::varchar

-- update datawarehouse.cepes_api_02
-- set nr_cep = concat( nr_cep,'0000')
-- where length(nr_cep::text) = 4

--cepes_api_04 coreção em sp

-- select nr_cep, sg_uf from datawarehouse.cepes_api_04
-- where length(nr_cep::text) < 8

-- atualizar dim_cep
 -- select distinct nr_cep, nr_latitude,nr_longitude
 -- from datawarehouse.cep_denovo
 -- order by nr_cep asc
 -- insert into datawarehouse.dim_cep (nr_cep,nr_latitude,nr_longitude)
 -- select distinct nr_cep, nr_latitude,nr_longitude
 -- from datawarehouse.cep_denovo

-- atualizar primeiro as chaves estrangeiras deixar em INT

--atualizar cep nas dimensões
--atualizar BAIRRO
-- alter table datawarehouse.dim_bairro
-- add column id serial primary key

-- UPDATE datawarehouse.dim_bairro dbr
-- SET nr_cep = dcep.id
-- FROM datawarehouse.dim_cep dcep
-- where dbr.nr_cep = dcep.nr_cep

-- UPDATE datawarehouse.dim_bairro dbr
-- SET cd_municipio = dm.id
-- FROM datawarehouse.dim_municipio dm
-- where dbr.cd_municipio = dm.cd_municipio

-- UPDATE datawarehouse.dim_bairro dbr
-- SET sg_uf = dsu.id
-- FROM datawarehouse.dim_sg_uf dsu
-- where dsu.sg_uf = dbr.sg_uf 

-- alter table datawarehouse.dim_bairro
-- alter column fk_sg_uf  type integer using fk_sg_uf ::integer

-- alter table datawarehouse.dim_bairro
-- alter column fk_nr_cep  type integer using fk_nr_cep ::integer

--ATUALIZAR ENDEREÇO

-- update datawarehouse.dim_endereco de
-- set nr_cep = dcep.id
-- from datawarehouse.dim_cep dcep
-- where dcep.nr_cep = de.nr_cep

-- update datawarehouse.dim_endereco de
-- set cd_municipio = dm.id
-- from datawarehouse.dim_municipio dm
-- where dm.cd_municipio = de.cd_municipio

-- update datawarehouse.dim_endereco de
-- set sg_uf = dsu.id
-- from datawarehouse.dim_sg_uf dsu
-- where dsu.sg_uf = de.sg_uf

-- update datawarehouse.dim_endereco de
-- set nm_bairro = dbr.id
-- from datawarehouse.dim_bairro dbr
-- where dbr.nm_bairro = de.nm_bairro

-- alter table datawarehouse.dim_endereco
-- add column id serial primary key

-- alter table datawarehouse.dim_endereco
-- alter column fk_nr_cep  type integer using fk_nr_cep ::integer

-- alter table datawarehouse.dim_endereco
-- alter column fk_sg_uf  type integer using fk_sg_uf ::integer

-- alter table datawarehouse.dim_endereco
-- alter column fk_nm_bairro  type integer using fk_nm_bairro ::integer

--ATUALIZAR ZONA

-- alter table datawarehouse.dim_zona
-- add column id serial primary key

-- update datawarehouse.dim_zona dz
-- set nr_cep = dcep.id
-- from datawarehouse.dim_cep dcep
-- where dcep.nr_cep = dz.nr_cep

-- update datawarehouse.dim_zona dz
-- set ds_endereco = de.id
-- from datawarehouse.dim_endereco de
-- where de.ds_endereco = dz.ds_endereco

-- update datawarehouse.dim_zona dz
-- set cd_municipio = dm.id
-- from datawarehouse.dim_municipio dm
-- where dz.cd_municipio = dm.cd_municipio

-- update datawarehouse.dim_zona dz
-- set sg_uf = dsu.id
-- from datawarehouse.dim_sg_uf dsu
-- where dsu.sg_uf = dz.sg_uf

-- update datawarehouse.dim_zona dz
-- set nm_bairro = dbr.id
-- from datawarehouse.dim_bairro dbr
-- where dbr.nm_bairro = dz.nm_bairro

-- alter table datawarehouse.dim_zona
-- alter column nr_cep  type integer using nr_cep ::integer

-- alter table datawarehouse.dim_zona
-- alter column ds_endereco  type integer using ds_endereco ::integer

-- alter table datawarehouse.dim_zona
-- alter column sg_uf  type integer using sg_uf ::integer

-- alter table datawarehouse.dim_zona
-- alter column nm_bairro  type integer using nm_bairro ::integer

14/12/2024

-- UPDATE datawarehouse.fato_eleicao fe 
-- set aa_eleicao = da.id
-- FROM datawarehouse.dim_ano da 
-- where da.ano = fe.aa_eleicao

-- UPDATE datawarehouse.fato_eleicao fe 
-- set sg_uf = dsu.id
-- FROM datawarehouse.dim_sg_uf dsu
-- where dsu.sg_uf = fe.sg_uf

-- UPDATE datawarehouse.fato_eleicao fe 
-- set cd_municipio = dm.id
-- FROM datawarehouse.dim_municipio dm
-- where dm.cd_municipio = fe.cd_municipio

-- UPDATE datawarehouse.fato_eleicao fe 
-- set nr_zona = dz.id
-- from datawarehouse.dim_zona dz
-- where dz.nr_zona = fe.nr_zona

-- UPDATE datawarehouse.fato_eleicao fe 
-- set nr_turno = dt.id
-- from datawarehouse.dim_turno dt	 
-- where dt.nr_turno = fe.nr_turno

-- UPDATE datawarehouse.fato_eleicao fe 
-- set nr_votavel = dl.id
-- from datawarehouse.dim_legenda dl	 
-- where dl.nr_votavel = fe.nr_votavel

-- UPDATE datawarehouse.fato_partido fp
-- set fk_dim_legenda = dl.id
-- from datawarehouse.dim_legenda dl
-- where dl.nr_votavel= fp.nr_partido

select * from datawarehouse.fato_partido
where fk_dim_legenda is null

select * from datawarehouse.fato_eleicao
where nr_votavel is null


alter table datawarehouse.dim_municipio
alter column fk_sg_uf type integer using fk_sg_uf::integer

select nr_votavel,nm_votavel from datawarehouse.fato_eleicao
where nm_votavel like'JOAO FRANCISCO ALBUQUERQUE DE OLIVEIRA'


update datawarehouse.dim_nome_candidato
set fk_dim_legenda =  dl.id
from datawarehouse.dim_legenda dl
where dl.nr_votavel = fk_dim_legenda


alter table datawarehouse.fato_cep
alter COLUMN fk_nr_cep type integer using fk_nr_cep::integer

alter table datawarehouse.fato_cep
alter COLUMN fk_nm_bairro type integer using fk_nm_bairro::integer

alter table datawarehouse.fato_cep
alter COLUMN fk_sg_uf type integer using fk_sg_uf::integer


alter table datawarehouse.fato_eleicao
alter COLUMN fk_sg_uf type integer using fk_sg_uf::integer

SELECT * FROM datawarehouse.fato_partido
where fk_dim_legenda is null

insert into datawarehouse.dim_legenda (nr_votavel)
values (97),(98),(99)

update datawarehouse.fato_partido
set fk_dim_legenda = dl.id
from datawarehouse.dim_legenda dl
where fk_dim_legenda is null
and nr_votavel = dl.nr_votavel

CONSULTA TOP!!!! PRO Bi  I!!
select distinct 
	da.ano,
	dt.nr_turno,
	dl.nr_votavel,
	fe.nm_votavel,
	dsu.sg_uf,
	dm.nm_municipio,
	fe.fk_nr_zona,
	sum(fe.qt_votos) AS total_votos
	FROM  datawarehouse.fato_eleicao fe
INNER JOIN datawarehouse.dim_ano da ON da.id = fe.fk_aa_eleicao
INNER JOIN datawarehouse.dim_turno dt ON dt.id = fe.fk_nr_turno	
INNER JOIN datawarehouse.dim_legenda dl ON dl.id = fe.fk_nr_votavel
INNER JOIN datawarehouse.dim_sg_uf dsu ON dsu.id = fe.fk_sg_uf
INNER JOIN datawarehouse.dim_municipio dm ON dm.id = fe.fk_cd_municipio
INNER JOIN datawarehouse.dim_zona dz ON dz.id = fe.fk_nr_zona 
WHERE dsu.sg_uf ilike 'CE' 
AND da.ano = 2016
aND dt.nr_turno = 1 
AND dm.nm_municipio ilike'Fortaleza'
GROUP BY da.ano, dt.nr_turno, dl.nr_votavel, fe.nm_votavel, dsu.sg_uf, dm.nm_municipio, fe.fk_nr_zona, fe.nm_votavel
ORDER BY (sum(fe.qt_votos)) DESC
LIMIT 100;

select distinct 
da.ano,
dcep.nr_cep,
dm.cd_municipio,
dsu.sg_uf,
dbr.nm_bairro,
dz.nr_zona
from datawarehouse.fato_cep fcep
inner join datawarehouse.dim_cep as dcep on dcep.id = fcep.fk_nr_cep
INNER JOIN datawarehouse.dim_ano as da on da.id = fcep.fk_aa_eleicao
INNER JOIN datawarehouse.dim_zona as dz on dz.id = fcep.fk_nr_zona
INNER JOIN datawarehouse.dim_bairro as dbr on dbr.id = fcep.fk_nm_bairro
INNER JOIN datawarehouse.dim_municipio as dm on dm.id = fcep.fk_cd_municipio
INNER JOIN datawarehouse.dim_sg_uf as dsu on dsu.id = fcep.fk_sg_uf

15_12_2024

--consulta 01
select distinct 
da.ano,
dcep.nr_cep,
dm.cd_municipio,
dsu.sg_uf,
dbr.nm_bairro,
dz.nr_zona
from datawarehouse.fato_cep fcep
inner join datawarehouse.dim_cep as dcep on dcep.id = fcep.fk_nr_cep
INNER JOIN datawarehouse.dim_ano as da on da.id = fcep.fk_aa_eleicao
INNER JOIN datawarehouse.dim_zona as dz on dz.id = fcep.fk_nr_zona
INNER JOIN datawarehouse.dim_bairro as dbr on dbr.id = fcep.fk_nm_bairro
INNER JOIN datawarehouse.dim_municipio as dm on dm.id = fcep.fk_cd_municipio
INNER JOIN datawarehouse.dim_sg_uf as dsu on dsu.id = fcep.fk_sg_uf

--consulta 02
select distinct 
	da.ano,
	dt.nr_turno,
	dl.nr_votavel,
	fe.nm_votavel,
	dsu.sg_uf,
	dp.nome_pais,
	dm.nm_municipio,
	dbr.nm_bairro,
	dcep.nr_cep,
	dcep.nr_latitude,
	dcep.nr_longitude,
	fe.fk_nr_zona,
	sum(fe.qt_votos) AS total_votos
	FROM  datawarehouse.fato_eleicao fe
INNER JOIN datawarehouse.dim_ano da ON da.id = fe.fk_aa_eleicao
INNER JOIN datawarehouse.dim_turno dt ON dt.id = fe.fk_nr_turno	
INNER JOIN datawarehouse.dim_legenda dl ON dl.id = fe.fk_nr_votavel
INNER JOIN datawarehouse.dim_sg_uf dsu ON dsu.id = fe.fk_sg_uf
INNER JOIN datawarehouse.dim_municipio dm ON dm.id = fe.fk_cd_municipio
INNER JOIN datawarehouse.dim_zona dz ON dz.id = fe.fk_nr_zona
inner join datawarehouse.dim_bairro dbr on dbr.id = dz.fk_nm_bairro
inner join datawarehouse.dim_cep dcep on dcep.id = dz.fk_nr_cep
inner join datawarehouse.dim_pais dp on dp.id = dsu.fk_pais
GROUP BY da.ano, dt.nr_turno, dl.nr_votavel, fe.nm_votavel, dsu.sg_uf, dm.nm_municipio,dbr.nm_bairro,
		 fe.fk_nr_zona,dcep.nr_cep,dcep.nr_latitude, dcep.nr_longitude, fe.nm_votavel,dp.nome_pais
ORDER BY (sum(fe.qt_votos)) DESC

-- consulta 03
select distinct 
	da.ano,
	dt.nr_turno,
	dl.nr_votavel,
	fe.nm_votavel,
	dsu.sg_uf,
	dm.nm_municipio,
	dbr.nm_bairro,
	fe.fk_nr_zona,
	sum(fe.qt_votos) AS total_votos
	FROM  datawarehouse.fato_eleicao fe
INNER JOIN datawarehouse.dim_ano da ON da.id = fe.fk_aa_eleicao
INNER JOIN datawarehouse.dim_turno dt ON dt.id = fe.fk_nr_turno	
INNER JOIN datawarehouse.dim_legenda dl ON dl.id = fe.fk_nr_votavel
INNER JOIN datawarehouse.dim_sg_uf dsu ON dsu.id = fe.fk_sg_uf
INNER JOIN datawarehouse.dim_municipio dm ON dm.id = fe.fk_cd_municipio
INNER JOIN datawarehouse.dim_zona dz ON dz.id = fe.fk_nr_zona
inner join datawarehouse.dim_bairro dbr on dbr.id = dz.fk_nm_bairro
WHERE dsu.sg_uf ilike 'CE' 
AND da.ano = 2016
aND dt.nr_turno = 1 
AND dm.nm_municipio ilike'Fortaleza'
GROUP BY da.ano, dt.nr_turno, dl.nr_votavel, fe.nm_votavel, dsu.sg_uf, dm.nm_municipio,dbr.nm_bairro, fe.fk_nr_zona, fe.nm_votavel
ORDER BY (sum(fe.qt_votos)) DESC
LIMIT 100;

03/01/2011
CREATE TABLE datawarehouse.dim_tabela_bi AS
SELECT 
    de.ds_endereco,
    dbr.nm_bairro,
    dm.nm_municipio,
    dsu.estado,
    dp.nome_pais,
    dcep.nr_cep,
    -- Concatenação 1: Estado e País
    TRIM(BOTH ',' FROM CONCAT(
        dsu.estado, ', ',
        dp.nome_pais
    )) AS endereco_completo_1,
    -- Concatenação 2: Município, Estado e País
    TRIM(BOTH ',' FROM CONCAT(
        dm.nm_municipio, ', ',
        dsu.estado, ', ',
        dp.nome_pais
    )) AS endereco_completo_2,
    -- Concatenação 3: Bairro, Município, Estado e País
    TRIM(BOTH ',' FROM CONCAT(
        dbr.nm_bairro, ', ',
        dm.nm_municipio, ', ',
        dsu.estado, ', ',
        dp.nome_pais
    )) AS endereco_completo_3,
    -- Concatenação Completa: Endereço, Bairro, Município, Estado e País
    TRIM(BOTH ',' FROM CONCAT(
        de.ds_endereco, ', ',
        dbr.nm_bairro, ', ',
        dm.nm_municipio, ', ',
        dsu.estado, ', ',
        dp.nome_pais
    )) AS endereco_completo
FROM datawarehouse.dim_endereco de
INNER JOIN datawarehouse.dim_cep AS dcep ON dcep.id = de.fk_nr_cep
INNER JOIN datawarehouse.dim_municipio AS dm ON dm.id = de.fk_cd_municipio
INNER JOIN datawarehouse.dim_sg_uf AS dsu ON dsu.id = de.fk_sg_uf
INNER JOIN datawarehouse.dim_bairro AS dbr ON dbr.id = de.fk_nm_bairro
INNER JOIN datawarehouse.dim_pais AS dp ON dp.id = dsu.fk_pais;


ALTER TABLE datawarehouse.dim_sg_uf
ADD COLUMN estado VARCHAR(100);

UPDATE datawarehouse.dim_sg_uf
SET estado = CASE 
    WHEN sg_uf = 'AC' THEN 'Acre'
    WHEN sg_uf = 'AL' THEN 'Alagoas'
    WHEN sg_uf = 'AM' THEN 'Amazonas'
    WHEN sg_uf = 'AP' THEN 'Amapa'
    WHEN sg_uf = 'BA' THEN 'Bahia'
    WHEN sg_uf = 'CE' THEN 'Ceara'
    WHEN sg_uf = 'DF' THEN 'Distrito Federal'
    WHEN sg_uf = 'ES' THEN 'Espirito Santo'
    WHEN sg_uf = 'GO' THEN 'Goias'
    WHEN sg_uf = 'MA' THEN 'Maranhao'
    WHEN sg_uf = 'MG' THEN 'Minas Gerais'
    WHEN sg_uf = 'MS' THEN 'Mato Grosso do Sul'
    WHEN sg_uf = 'MT' THEN 'Mato Grosso'
    WHEN sg_uf = 'PA' THEN 'Para'
    WHEN sg_uf = 'PB' THEN 'Paraiba'
    WHEN sg_uf = 'PE' THEN 'Pernambuco'
    WHEN sg_uf = 'PI' THEN 'Piaui'
    WHEN sg_uf = 'PR' THEN 'Parana'
    WHEN sg_uf = 'RJ' THEN 'Rio de Janeiro'
    WHEN sg_uf = 'RN' THEN 'Rio Grande do Norte'
    WHEN sg_uf = 'RO' THEN 'Rondonia'
    WHEN sg_uf = 'RR' THEN 'Roraima'
    WHEN sg_uf = 'RS' THEN 'Rio Grande do Sul'
    WHEN sg_uf = 'SC' THEN 'Santa Catarina'
    WHEN sg_uf = 'SE' THEN 'Sergipe'
    WHEN sg_uf = 'SP' THEN 'Sao Paulo'
    WHEN sg_uf = 'TO' THEN 'Tocantins'
    ELSE NULL -- Opcional
END;

06/01/2024

SELECT * FROM datawarehouse.dim_bairro
ORDER BY fk_cd_municipio
296593

DELETE FROM  datawarehouse.dim_municipio
WHERE id BETWEEN 296598 AND 307847;

SELECT * FROM datawarehouse.dim_municipio
order by id
where cd_municipio = 86460


--Atualizar as Tabelas Relacionadas
UPDATE datawarehouse.dim_zona AS fato
SET fk_cd_municipio = (
    SELECT MIN(id)
    FROM datawarehouse.dim_municipio AS dim
    WHERE fato.fk_cd_municipio = dim.id
    GROUP BY nm_municipio, fk_sg_uf, cd_municipio
)
WHERE fk_cd_municipio IN (
    SELECT id
    FROM datawarehouse.dim_municipio
    WHERE id NOT IN (
        SELECT MIN(id)
        FROM datawarehouse.dim_municipio
        GROUP BY nm_municipio, fk_sg_uf, cd_municipio
    )
);



-- SQL para Remover Duplicatas
DELETE FROM datawarehouse.dim_municipio
WHERE id NOT IN (
    SELECT MIN(id)
    FROM datawarehouse.dim_municipio
    GROUP BY 
        nm_municipio, 
        fk_sg_uf, 
        cd_municipio
);
--Identificar Duplicatas
SELECT 
    nm_municipio, 
    fk_sg_uf, 
    cd_municipio, 
    MIN(id) AS menor_id, 
    COUNT(*) AS total
FROM 
    datawarehouse.dim_municipio
GROUP BY 
    nm_municipio, 
    fk_sg_uf, 
    cd_municipio
HAVING 
    COUNT(*) > 1;
	


UPDATE datawarehouse.dim_municipio AS dm
SET cd_municipio = fe.cd_municipio
from datawarehouse.cep_denovo fe
where fe.nm_municipio = dm.nm_municipio AND 
dm.cd_municipio is null

UPDATE datawarehouse.dim_municipio AS dm
SET 
    nm_municipio = SPLIT_PART(dm.estado_municipio, ',', 1),
    fk_sg_uf = (
        SELECT id
        FROM datawarehouse.dim_sg_uf AS dsu
        WHERE dsu.estado = TRIM(SPLIT_PART(dm.estado_municipio, ',', 2))
    )
WHERE dm.nm_municipio IS NULL;



SELECT nm_municipio FROM datawarehouse.dim_municipio
where nm_municipio ilike 'nova fatima'
order by id




select * from datawarehouse.fato_partido
order by id

DELETE FROM  datawarehouse.fato_partido
WHERE id BETWEEN 39 AND 76;
-- ELEITOS E NÃO ELEITOS EM PRIMEIRO TURNO

WITH total_votos_1_turno_municipio AS (
  SELECT 
    dsu.estado AS estado,
    dm.nm_municipio AS municipio,
    da.ano AS ano,
    SUM(fe.qt_votos) AS total_votos
  FROM datawarehouse.fato_eleicao fe
  JOIN datawarehouse.dim_legenda dl ON dl.id = fe.fk_nr_votavel
  JOIN datawarehouse.dim_municipio dm ON dm.id = fe.fk_cd_municipio
  JOIN datawarehouse.dim_ano da ON da.id = fe.fk_aa_eleicao
  JOIN datawarehouse.dim_turno dt ON dt.id = fe.fk_nr_turno
  JOIN datawarehouse.dim_sg_uf dsu ON dsu.id = fe.fk_sg_uf
  WHERE dt.nr_turno = 1
  GROUP BY dsu.estado, dm.nm_municipio, da.ano
),
qtd_votos_partido_ano AS (
  SELECT 
    da.ano AS ano,
    dsu.estado AS estado,
    dm.nm_municipio AS municipio,
    dl.nr_votavel AS partido,
    SUM(fe.qt_votos) AS votos_partido
  FROM datawarehouse.fato_eleicao fe
  JOIN datawarehouse.dim_legenda dl ON dl.id = fe.fk_nr_votavel
  JOIN datawarehouse.dim_municipio dm ON dm.id = fe.fk_cd_municipio
  JOIN datawarehouse.dim_ano da ON da.id = fe.fk_aa_eleicao
  JOIN datawarehouse.dim_turno dt ON dt.id = fe.fk_nr_turno
  JOIN datawarehouse.dim_sg_uf dsu ON dsu.id = fe.fk_sg_uf
  WHERE dt.nr_turno = 1
  GROUP BY da.ano, dsu.estado, dm.nm_municipio, dl.nr_votavel
)
SELECT 
  q.ano,
  q.estado,
  q.municipio,
  q.partido,
  q.votos_partido,
  t.total_votos,
  ROUND(q.votos_partido::numeric * 100 / t.total_votos, 2) AS percentual_votos,
  CASE
    WHEN ROUND(q.votos_partido::numeric * 100 / t.total_votos, 2) > 50 THEN true
    ELSE false
  END AS eleito_1_turno
FROM qtd_votos_partido_ano q
JOIN total_votos_1_turno_municipio t
  ON q.ano = t.ano
  AND q.estado = t.estado
  AND q.municipio = t.municipio
ORDER BY eleito_1_turno DESC, percentual_votos DESC;








			



















