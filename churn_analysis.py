from db_utils.sqlite_connect import sqlite_connect
from db_utils.pg_connect import pg_connect

#db = sqlite_connect('churn.db')
db = pg_connect('docker')


orders = db.get_df_from_query('''
SELECT min(ts) FROM orders;
    ''', pprint=True)

print(orders)


monthly_view = db.get_df_from_query('''
SELECT 
    *
FROM customer_monthly
    ''', pprint=True)

print(monthly_view)

date_utils = db.get_df_from_query('''
SELECT 
    *
FROM date_utils
    ''', pprint=True)

print(date_utils)

customer_dates = db.get_df_from_query('''
SELECT * FROM customer_dates
    ''', pprint=True)

print(customer_dates)

monthly_states = db.get_df_from_query('''
SELECT 
    c.customer as customer,
    c.first_order as first_order_date,
    strftime('%Y-%m', ts) as year_month,
    orders,
    total,
    CASE
        when strftime('%Y-%m', c.first_order) = strftime('%Y-%m', ts) then 'NEW'
        else null
    END as status
FROM customer_dates as c
LEFT JOIN customer_monthly as cm
ON (c.customer = cm.customer and strftime('%Y-%m', ts) = month_year)
WHERE strftime('%Y-%m', c.first_order) <= strftime('%Y-%m', ts)
    ''', pprint=True)

print(monthly_states)


month = db.get_df_from_query('''
select val, rank() over(order by Val) ValRank FROM RankDemo;
    ''', pprint=True)


print(month)