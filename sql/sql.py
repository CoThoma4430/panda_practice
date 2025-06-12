import duckdb

con = duckdb.connect(database=':memory:')

customers = './5784/noahs-customers.csv'
orders = './5784/noahs-orders.csv'

query = f"""
select
    c.customerid,
    c.name,
    o.ordered
from
    read_csv_auto ("{customers}") c
left join
    read_csv_auto ("{orders}") o
on
 c.customerid = o.customerid
"""

df = con.execute(query).fetchdf()
print(df)
con.close()
