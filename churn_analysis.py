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


month = db.get_df_from_query('''
WITH timeseries AS
(
  SELECT mes.customer,
         COALESCE(ts,current_purchase_month) AS MONTH,
         current_purchase_month,
         first_purchase_month,
         next_purchase_month,
         last_purchase_month,
         orders,
         status,
         (COALESCE(ts,current_purchase_month) - last_purchase_month) / 30 AS months_since_last_purchase
  FROM monthly_econ_states AS mes
    LEFT JOIN customer_dates AS cd
           ON (cd.customer = mes.customer
          AND cd.ts >= mes.current_purchase_month
          AND cd.ts <COALESCE (mes.next_purchase_month,CURRENT_DATE))
  ORDER BY 1,
           2
)
SELECT *,
       CASE
         WHEN first_purchase_month = month THEN 'NEW'
         WHEN (month- current_purchase_month) / 30 < 3 THEN 'ACTIVE'
         WHEN (month- current_purchase_month) / 30 = 3 THEN 'CHURN'
         WHEN (month- current_purchase_month) / 30 > 3 THEN NULL
         ELSE status
       END 
FROM timeseries;

    ''', pprint=True)


print(month)