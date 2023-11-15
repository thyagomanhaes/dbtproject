WITH prod AS (
SELECT 
    ct.category_name,
    sp.company_name suppliers,
    pd.product_name,
    pd.unit_price,
    pd.product_id
FROM {{source('sources', 'products')}} pd 
LEFT JOIN {{source('sources', 'suppliers')}} sp on (pd.supplier_id = sp.supplier_id)
LEFT JOIN {{source('sources', 'categories')}} ct on (pd.supplier_id = ct.category_id)
), orddetai AS (
    select pd.*, od.order_id, od.quantity, od.discount 
    from {{ref('orderdetails')}} od 
    left join prod pd on (od.product_id = pd.product_id)
),
ordrs AS (
    select
        ord.order_date,
        ord.order_id,
        cs.company_name customer,
        em.name as employee,
        em.age,
        em.lenghtofservice
    from {{source('sources', 'orders')}} ord 
    left join {{ref('customers')}} cs on (ord.customer_id = cs.customer_id)
    left join {{ref('employees')}} em on (ord.employee_id = em.employee_id)
    left join {{source('sources', 'shippers')}} sh on (ord.ship_via = sh.shipper_id)
), finaljoin AS (
    select od.*, ord.order_date, ord.customer, ord.employee
    from orddetai od 
    inner join ordrs ord on (od.order_id = ord.order_id)
)
select * from finaljoin
limit 5000 