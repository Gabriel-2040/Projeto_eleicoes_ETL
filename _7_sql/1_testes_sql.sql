1- 
SELECT distinct
	nm_votavel,a
	ds_cargo,
	sg_uf
from public.eleicoes_2020
where ds_cargo ='Prefeito' and sg_uf ilike 'ce'
2-
SELECT distinct
	nm_votavel,
	ds_cargo,
	sg_uf,
	nm_municipio
from public.eleicoes_2020
where ds_cargo ='Prefeito' and sg_uf ilike 'ce'


select *
	from public.eleicoes_2020
	where nm_votavel ilike 'JOSÉ SARTO NOGUEIRA MOREIRA'

select nm_votavel
	from dw.dcandidato
	where nm_votavel ilike '%josé sarto%'

select * from dw.d_municipio
	where uf ilike 'ce'

select * from dw.d_municipio
	where uf ilike 'ce'

select sum(qt_votos) from dw.f_eleicoes

select * from dw.tipo_candidatura as tc
where tc.ds_cargo ilike 'prefeito'
group by tc.ds_cargo, tc.cd_municipio

-- modifiquei as dw pois como não estavam serialziadas a consulta demorava tanto que dava erro.
-- precisei colocar id com a confiança do cd_municipio pra garantir que todos teriam a mesma quantidade de id
-- só asism deu certo. Modifiquei no pentahoo também pra fazer ja o processo de serialização lá.
-- ai aqui embaixo a primeira consulta. agora tentar as outras.
-- total de votos por candidatos a prefeitos no ceara ordenados por cidade e por quantidade de votos
select distinct
	dc.nm_votavel,
	dm.sg_uf,
	sum(fe.qt_votos) as soma_voto,
	tc.ds_cargo,
	dm.nm_municipio
from dw.dtipo_candidatura as tc
	inner join dw.feleicoes as fe on fe.id = tc.id
	inner join dw.d_municipio as dm on dm.id = tc.id
	inner join dw.dcandidato as dc on dc.id = tc.id
where dm.sg_uf ilike 'ce' and tc.ds_cargo ilike 'prefeito'
group by dc.nm_votavel, dm.sg_uf, tc.ds_cargo, dm.nm_municipio
order by dm.nm_municipio, soma_voto desc
-- escolhendo o prefeito mais votado por municipio. tem que
-- ordenar na descendencia se não o DISTINCT ON escolhe o ultimo.
SELECT DISTINCT ON (dm.nm_municipio)
    dc.nm_votavel,
    dm.sg_uf,
    sum(fe.qt_votos) as soma_voto,
    tc.ds_cargo,
    dm.nm_municipio
FROM dw.dtipo_candidatura as tc
    INNER JOIN dw.feleicoes as fe ON fe.id = tc.id
    INNER JOIN dw.d_municipio as dm ON dm.id = tc.id
    INNER JOIN dw.dcandidato as dc ON dc.id = tc.id
WHERE dm.sg_uf ILIKE 'ce' 
  AND tc.ds_cargo ILIKE 'prefeito'
GROUP BY dc.nm_votavel, dm.sg_uf, tc.ds_cargo, dm.nm_municipio
ORDER BY dm.nm_municipio, soma_voto DESC;
	


	alter table dw.dcandidato 
	ADD column if not exists cd_municipio BIGINT;
INSERT INTO dw.dcandidato (cd_municipio)
SELECT cd_municipio
FROM public.eleicoes_2020;


SELECT distinct
	nm_votavel,
	ds_cargo,
	sg_uf,
	sum(qt_votos) as soma,
	nm_municipio
from public.eleicoes_2020
where ds_cargo ='Prefeito' and sg_uf ilike 'ce'
	group by eleicoes_2020.nm_votavel, eleicoes_2020.ds_cargo, eleicoes_2020.sg_uf, eleicoes_2020.nm_municipio
order by soma desc
	
-- total de votos por prefeitos no ceara

select distinct
	dc.nm_votavel,
	dtc.ds_cargo,
	dm.sg_uf,
	
from dw.feleicoes fe
inner join dw.dcandidato as dc on dc.cd_municipio = fe.cd_municipio
inner join dw.dtipo_candidatura as dtc on dtc.cd_municipio = fe.cd_municipio
inner join dw.d_municipio as dm on dm.cd_municipio = fe.cd_municipio
where dtc.ds_cargo ='Prefeito' and dm.sg_uf ilike 'CE' 
group by dm.sg_uf, dm.nm_municipio, dc.nm_votavel, dtc.ds_cargo
order by soma


SELECT
    cd_municipio,
    sum(fe.qt_votos)
FROM dw.feleicoes fe
GROUP BY cd_municipio;
 
WITH
    eleicoes_municipios AS (
        -- Seleciona apenas os dados relevantes da tabela de eleições
        SELECT
            fe.cd_municipio,
            sum(fe.qt_votos) as soma
        FROM dw.feleicoes fe
		group by fe.cd_municipio
    ),
    municipios AS (
        -- Seleciona apenas os municípios do Ceará
        SELECT
            dm.cd_municipio,
            dm.sg_uf,
            dm.nm_municipio
        FROM dw.d_municipio dm
        WHERE dm.sg_uf ILIKE 'CE'
    ),
    candidatos AS (
        -- Filtra candidatos com cargo de Prefeito
        SELECT
            dc.cd_municipio,
            dc.nm_votavel
        FROM dw.dcandidato dc
    ),
    tipos_candidatura AS (
        -- Filtra para o cargo de Prefeito
        SELECT
            dtc.cd_municipio,
            dtc.ds_cargo
        FROM dw.dtipo_candidatura dtc
        WHERE dtc.ds_cargo = 'Prefeito'
    )
SELECT
    c.nm_votavel,
    tc.ds_cargo,
	m.soma,
    m.sg_uf,
    m.nm_municipio
FROM eleicoes_municipios em
JOIN municipios m ON em.cd_municipio = m.cd_municipio
JOIN candidatos c ON em.cd_municipio = c.cd_municipio
JOIN tipos_candidatura tc ON em.cd_municipio = tc.cd_municipio
GROUP BY c.nm_votavel, tc.ds_cargo, m.sg_uf, m.nm_municipio
ORDER BY soma;



