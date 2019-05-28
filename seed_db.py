from db_utils.sqlite_connect import sqlite_connect
from db_utils.pg_connect import pg_connect

#db = sqlite_connect('churn.db')

from db_utils.sqlite_connect import sqlite_connect
import csv


db = pg_connect('docker')

db.update_db('''DROP TABLE IF EXISTS orders CASCADE''', pprint=True)

db.update_db('''DROP TABLE IF EXISTS date_utils''', pprint=True)

db.update_db('''DROP VIEW IF EXISTS customer_monthly''', pprint=True)

db.update_db('''DROP VIEW IF EXISTS customer_dates''', pprint=True)

db.update_db('''
    CREATE TABLE orders(
    customer varchar, 
    ts date, 
    total float)''', pprint=True)

db.update_db('''
    CREATE TABLE date_utils( 
    ts date)''', pprint=True)


with open('orders.csv', 'r') as csvfl:
    orders = csv.reader(csvfl, delimiter=',')
    for i in orders:
        
        db.update_db('''
        INSERT INTO orders(customer, ts, total)
        VALUES(%s, %s, %s)
        ''',pprint=True, params=(tuple(i))
        )
        print(i)

with open('date_utils.csv', 'r') as csvfl:
    dates = csv.reader(csvfl, delimiter=',')
    for i in dates:
        
        db.update_db('''
        INSERT INTO date_utils(ts)
        VALUES(%s)
        ''',pprint=True, params=(tuple(i))
        )
        print(i)


db.update_db('''
CREATE VIEW customer_monthly 
AS
SELECT *,
       FIRST_VALUE(current_purchase_month) OVER (PARTITION BY customer) first_purchase_month,
       LEAD(current_purchase_month) OVER (PARTITION BY customer) next_purchase_month,
       LAG(current_purchase_month) OVER (PARTITION BY customer) last_purchase_month
FROM (SELECT customer,
             TO_CHAR(ts,'YYYY-MM-01')::DATE AS current_purchase_month,
             COUNT(*) AS orders,
             SUM(total) AS total
      FROM orders
      GROUP BY 1,
               2
      ORDER BY 1,
               2) AS a;
    ''', pprint=True)


db.update_db('''
CREATE VIEW customer_dates AS SELECT 
    *
FROM (SELECT DISTINCT customer FROM orders GROUP BY 1) c
LEFT JOIN date_utils ON (1=1)
    ''', pprint=True)


db.update_db('''
CREATE VIEW monthly_econ_states 
AS
SELECT *,
       CASE
         WHEN current_purchase_month = first_purchase_month THEN 'NEW' 
         WHEN (current_purchase_month - last_purchase_month) / 30 <= 3 THEN 'ACTIVE'
         WHEN (current_purchase_month - last_purchase_month) / 30 > 3 THEN 'RETURNED'         
       END as status,
       (current_purchase_month - last_purchase_month) / 30
FROM customer_monthly;
    ''', pprint=True)