from db_utils.pg_connect import pg_connect
import csv


db = pg_connect(config_file='databases.conf', db_name='postgres')


#seed database with orders data
db.update_db('''DROP TABLE IF EXISTS orders CASCADE''', pprint=True)

db.update_db('''DROP TABLE IF EXISTS everyday''', pprint=True)

db.update_db('''DROP VIEW IF EXISTS customer_monthly''', pprint=True)

db.update_db('''DROP VIEW IF EXISTS customer_dates''', pprint=True)


db.update_db('''
    CREATE TABLE orders(
    customer varchar, 
    ts date, 
    total float)''', pprint=True)


db.update_db('''
    CREATE TABLE everyday( 
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
INSERT INTO everyday
SELECT (DATE '2018-10-01' +(GENERATE_SERIES(0,245)))::DATE;
''',pprint=True)


db.update_db('''
CREATE VIEW customer_dates_everyday 
AS
SELECT *
FROM (SELECT DISTINCT customer FROM orders GROUP BY 1) c
  LEFT JOIN everyday ON (1 = 1);
''', pprint=True)


db.update_db('''
CREATE VIEW customer_facts 
AS
SELECT *,
       FIRST_VALUE(ts) OVER (PARTITION BY customer order by ts) first_purchase,
       LEAD(ts) OVER (PARTITION BY customer order by ts) next_purchase,
       LAG(ts) OVER (PARTITION BY customer order by ts) previous_purchase
FROM orders;
''', pprint=True)

db.update_db('''
CREATE VIEW customer_econ_states
AS
SELECT *,
       CASE
         WHEN ts = first_purchase THEN 'NEW'
         WHEN (ts - first_purchase) < 30 THEN 'NEW'
         WHEN (ts - previous_purchase) <= 90 THEN 'ACTIVE'
         WHEN (ts - previous_purchase) > 90 THEN 'RETURNED'
       END AS status
FROM customer_facts
ORDER BY customer,
         ts;
  ''', pprint=True)


month = db.get_df_from_query('''
WITH timeseries AS
(
  SELECT mes.customer,
         ed.ts as day,
         mes.ts as current_purchase,
         first_purchase,
         next_purchase,
         previous_purchase,
         status,
         (ed.ts - previous_purchase) AS days_since_last_purchase
  FROM customer_econ_states AS mes
    LEFT JOIN everyday AS ed
           ON (ed.ts >= mes.ts
          AND ed.ts <COALESCE (mes.next_purchase,CURRENT_DATE))
  ORDER BY 1,
           2
)
SELECT customer, day, current_purchase, first_purchase, previous_purchase,
       CASE
         WHEN day = first_purchase THEN 'NEW'
         WHEN (day - first_purchase) < 30 THEN 'NEW'
         WHEN (current_purchase - previous_purchase) > 90 AND current_purchase = day THEN 'RETURNED'         
         WHEN (day - current_purchase) < 90 THEN 'ACTIVE'
         WHEN (day- current_purchase) = 90 THEN 'CHURN'
         WHEN (day- current_purchase) > 90 THEN NULL
         ELSE status
       END as status_through_time
        
       
FROM timeseries;
''', pprint=True)


print(month)

