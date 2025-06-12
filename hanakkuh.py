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
orders_items_path = './5784/noahs-orders_items.csv'
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


### part 2

query = f"""
select
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

orders_items_df = pd.read_csv('./5784/noahs-orders_items.csv')

products_df = pd.read_csv('./5784/noahs-products.csv')

# orders_items_df.info()
# print(orders_items_df['sku'])

products_df.info()
print(products_df['desc'])

df = con.execute(query).fetchdf()
var = pd.to_datetime(df['ordered']).dt.year
mask = var == 2017

initials = df['name'].apply(initialize)
mask = mask & (initials == 'JP')
# print(df[mask])


con.close()

