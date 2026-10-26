{{ config(alias = 'users_info') }}

SELECT  id
,       nome
,       SPLIT_PART(nome, ' ', 1)                                      AS primeiro_nome
,       CASE
            WHEN POSITION(' ' IN nome) = 0 THEN NULL
            ELSE SPLIT_PART(nome, ' ', -1)
        END                                                           AS ultimo_nome
,       email
,       SPLIT_PART(email, '@', 2)                                     AS dominio_email
,       idade
,       cidade
,       created_at
,       updated_at
FROM    {{ ref('stg_users_info') }}