import pandas as pd
import duckdb

con = duckdb.connect(database=':memory:')

customers = './5784/noahs-customers.csv'
orders = './5784/noahs-orders.csv'
items_path = './5784/noahs-orders_items.csv'
products_path = './5784/noahs-products.csv'

customers_df = pd.read_csv('./5784/noahs-customers.csv')
customers_df.info()

orders_df = pd.read_csv('./5784/noahs-orders.csv')
orders_df.info()

items_df = pd.read_csv('./5784/noahs-orders_items.csv')
items_df.info()

products_df = pd.read_csv('./5784/noahs-products.csv')
products_df.info()
