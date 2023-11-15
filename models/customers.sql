WITH markup AS (
SELECT
    *,
    first_value(customer_id)
OVER (PARTITION BY company_name, contact_name
ORDER BY company_name
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS RESULT    
FROM {{source('sources', 'customers')}}
),
removed AS (
     select distinct result from markup
),
final AS (
    select * FROM {{source('sources', 'customers')}} where customer_id in (select result from removed)
)
select * from final