{{
    config(
        materialized='table'
    )
}}

select 
*
from {{source('sources', 'categories')}}
