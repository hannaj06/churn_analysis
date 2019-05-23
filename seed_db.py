from db_utils.sqlite_connect import sqlite_connect
import csv


db = sqlite_connect('churn.db')

db.update_db('''DROP TABLE IF EXISTS orders''', pprint=True)

db.update_db('''DROP TABLE IF EXISTS date_utils''', pprint=True)

db.update_db('''DROP VIEW IF EXISTS customer_monthly''', pprint=True)

db.update_db('''DROP VIEW IF EXISTS customer_dates''', pprint=True)

db.update_db('''
    CREATE TABLE orders(
    customer varchar, 
    ts date, 
    total number)''', pprint=True)

db.update_db('''
    CREATE TABLE date_utils( 
    ts date)''', pprint=True)


with open('orders.csv', 'r') as csvfl:
    orders = csv.reader(csvfl, delimiter=',')
    for i in orders:
        
        db.update_db('''
        INSERT INTO orders(customer, ts, total)
        VALUES(?, ?, ?)
        ''',pprint=True, params=(tuple(i))
        )
        print(i)

with open('date_utils.csv', 'r') as csvfl:
    dates = csv.reader(csvfl, delimiter=',')
    for i in dates:
        
        db.update_db('''
        INSERT INTO date_utils(ts)
        VALUES(?)
        ''',pprint=True, params=(tuple(i))
        )
        print(i)


db.update_db('''
CREATE VIEW customer_monthly AS
SELECT 
    customer, 
    strftime('%Y-%m', ts) as month_year, 
    min(ts) as first_order,
    max(ts) as last_order,
    count(*) as orders, 
    sum(total) as total
FROM orders
GROUP BY 1, 2
ORDER BY 1, 2 
    ''', pprint=True)


db.update_db('''
CREATE VIEW customer_dates AS SELECT 
    *
FROM (select distinct customer, first_order from orders)
LEFT JOIN date_utils ON (1=1)
    ''', pprint=True)