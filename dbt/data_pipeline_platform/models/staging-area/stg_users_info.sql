{{ config(alias = 'users_info') }}

SELECT  id
,       UPPER(
            TRIM(
                REGEXP_REPLACE(nome, '^\s*(sr|sra|dr|dra)\.?\s+', '', 'i')
            )
        )                       AS nome
,       UPPER(TRIM(email))      AS email
,       idade
,       UPPER(TRIM(cidade))     AS cidade
,       NOW()                   AS created_at
,       NOW()                   AS updated_at
FROM    {{ ref('raw_users_info') }}