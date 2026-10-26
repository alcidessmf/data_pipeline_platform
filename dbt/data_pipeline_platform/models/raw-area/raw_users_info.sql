{{ config(alias = 'users_info') }}

SELECT  id
,       nome
,       email
,       idade
,       cidade
FROM    {{ source('xptoz_dw', 'users_info') }}