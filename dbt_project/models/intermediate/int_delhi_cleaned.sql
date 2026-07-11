-- Intermediate : passthrough sur le staging validé (déduplication/enrichissement à ajouter si besoin)

select * from {{ ref('stg_delhi_measurements') }}
where is_valid = true
