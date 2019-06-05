from db_utils.pg_connect import pg_connect
import csv


db = pg_connect(config_file='databases.conf', db_name='postgres')


#seed database with orders data
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
    ts date);
    ''', pprint=True)


with open('orders.csv', 'r') as csvfl:
    orders = csv.reader(csvfl, delimiter=',')
    for i in orders:
        
        db.update_db('''
        INSERT INTO orders(customer, ts, total)
        VALUES(%s, %s, %s)
        ''',pprint=True, params=(tuple(i))
        )
        print(i)
#----------------------------------------------------------------------------

db.update_db('''
INSERT INTO date_utils(ts)
SELECT (DATE '2018-10-01' +(INTERVAL '1' month*GENERATE_SERIES(0,8)))::DATE
''',pprint=True)



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
       END as status
FROM customer_monthly;
    ''', pprint=True)
db = pg_connect(config_file='databases.conf', db_name='postgres')

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


month = db.get_df_from_query('''
WITH timeseries ASgit st
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
    LEFT JOIN date_utils AS cd
           ON (cd.ts >= mes.current_purchase_month
          AND cd.ts <COALESCE (mes.next_purchase_month,CURRENT_DATE))
  ORDER BY 1,
           2
)
SELECT customer, month, 
       CASE
         WHEN first_purchase_month = month THEN 'NEW'
         WHEN (current_purchase_month - last_purchase_month)/ 30 > 3 AND current_purchase_month = month THEN 'RETURNED'         
         WHEN (month- current_purchase_month) / 30 < 3 THEN 'ACTIVE'
         WHEN (month- current_purchase_month) / 30 = 3 THEN 'CHURN'
         WHEN (month- current_purchase_month) / 30 > 3 THEN NULL
         ELSE status
       END as status_through_time
        
       
FROM timeseries;


    ''', pprint=True)


print(month)


print(db.get_df_from_query('SELECT * from monthly_econ_states', pprint=True))