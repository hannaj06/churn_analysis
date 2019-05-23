from db_utils.sqlite_connect import sqlite_connect

db = sqlite_connect('churn.db')

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
SELECT * 
FROM customer_dates as c
LEFT JOIN customer_monthly as cm
ON (c.customer = cm.customer and strftime('%Y-%m', ts) = month_year)
    ''', pprint=True)

print(monthly_states)