{{ config(alias = 'users_por_dominio') }}

SELECT  dominio_email
,       COUNT(*)            AS total_registros
,       NOW()               AS created_at
,       NOW()               AS updated_at
FROM    {{ ref('int_users_info') }}
GROUP BY dominio_email