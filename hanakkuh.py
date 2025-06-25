import pandas as pd
import duckdb

def convert_name_t9(name):

    t9name  = ''
    t9 = {
        '2': 'abc',
        '3': 'def',
        '4': 'ghi',
        '5': 'jkl',
        '6': 'mno',
        '7': 'pqrs',
        '8': 'tuv',
        '9': 'wxyz',
    }

    for letter in name.split()[-1].lower():
        for num, letters in t9.items():
            if letter in letters:
                t9name += num
                break
    
    return t9name

def initialize(full_name):
    initials = ''

    for name in full_name.split():
        initials += name[0]
    
    return initials

con = duckdb.connect(database=':memory:')

customers_path = './5784/noahs-customers.csv'
orders_path = './5784/noahs-orders.csv'
items_path = './5784/noahs-orders_items.csv'
products_path = './5784/noahs-products.csv'

### part 1

customers_df = pd.read_csv('./5784/noahs-customers.csv')
# customers_df.info()
# print(customers_df['phone'])
# print(customers_df.head())

t9_names = customers_df['name'].apply(convert_name_t9)
pn_stripped = customers_df['phone'].str.replace('-', '')

mask = t9_names == pn_stripped
# print(customers_df[mask]['phone'])

orders_df = pd.read_csv('./5784/noahs-orders.csv')
# orders_df.info()
# print(orders_df)

### part 2

### customers and orders query
query_ordered = f"""
select
    o.orderid,
    c.customerid,
    c.name,
    o.ordered
from
    read_csv_auto ("{customers_path}") c
left join
    read_csv_auto ("{orders_path}") o
on
 c.customerid = o.customerid
"""

### items dataframe creation
items_df = pd.read_csv('./5784/noahs-orders_items.csv')
# items_df.info()
# print(items_df)

### products dataframe creation
products_df = pd.read_csv('./5784/noahs-products.csv')
# products_df.info()
# print(products_df['desc'])
# print(products_df['sku'])

### items and products query
query_products = f"""
select
    o.orderid,
    o.sku,
    p.desc
from
    read_csv_auto ("{items_path}") o
left join
    read_csv_auto ("{products_path}") p
on
 o.sku = p.sku
"""

### combination of query_ordered and query_products
query_combined = f"""
select
    o.orderid,
    c.customerid,
    c.name,
    c.phone,
    o.ordered,
    i.sku,
    p.desc
from
    read_csv_auto ("{customers_path}") c
left join
    read_csv_auto ("{orders_path}") o
    on c.customerid = o.customerid
left join
    read_csv_auto ("{items_path}") i
    on o.orderid = i.orderid
left join
    read_csv_auto ("{products_path}") p
    on i.sku = p.sku
where
    extract(year from o.ordered::timestamp) = 2017
    and regexp_replace(c.name, '([A-Z])[a-z]* ?', '\\1', 'g') = 'JP'
    and lower(p.desc) like '%coffee%'
"""
    

### mask to find coffee sku inside products dataframe
# mask = products_df['desc'].str.contains('coffee', case=False, na=False)
# filtered_df = products_df[mask]
# print(products_df[mask])

### mask to find all the orderid's that got coffee
df_prodorder = con.execute(query_products).fetchdf()
mask_prodorder = df_prodorder['desc'].str.contains('coffee', case=False, na=False)
# print(df_prodorder[mask_prodorder])

### mask to find all of the JP's who ordered in 2017
df = con.execute(query_ordered).fetchdf()
var = pd.to_datetime(df['ordered']).dt.year
mask = var == 2017
initials = df['name'].apply(initialize)
mask = mask & (initials == 'JP')
# print(df[mask])

### answer to candle 2
combined_df = con.execute(query_combined).fetchdf()
# print(combined_df)

### Candle 3

# print(customers_df['birthdate'])

year = 1903

years = []
while year < 2020:
    years.append(year)
    year += 12

query_cancer = f"""
select
    c.customerid,
    COUNT(o.orderid) = 0 as no_orders
from
    read_csv_auto ("{customers_path}") c
left join
    read_csv_auto ("{orders_path}") o
on
    c.customerid = o.customerid
group by 
    c.customerid
"""

df_result = con.execute(query_cancer).fetchdf()

customers_df['years'] = pd.to_datetime(customers_df['birthdate']).dt.year
customers_df['rabbit'] = customers_df['years'].isin(years)
customers_df['month'] = pd.to_datetime(customers_df['birthdate']).dt.month
customers_df['day'] = pd.to_datetime(customers_df['birthdate']).dt.day
mask_june_cancer = (customers_df['month'] == 6) & (customers_df['day'] >= 21)
mask_july_cancer = (customers_df['month'] == 7) & (customers_df['day'] <= 22)
mask_cancer = mask_june_cancer | mask_july_cancer
birthdate_mask = mask_cancer & customers_df['rabbit']
# print(customers_df[birthdate_mask & df_result['no_orders']])


query_address = f"""
select
    address
from
    read_csv_auto ("{customers_path}") c
where
    customerid = 1475
"""

df_address = con.execute(query_address).fetchdf()
# print(df_address)
customers_df.info()

con.close()





