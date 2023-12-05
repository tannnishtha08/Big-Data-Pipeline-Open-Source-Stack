
# importing the libraries
import pandas as pd 
import numpy as np
import mysql.connector

# loading the datasets
product = pd.read_csv("create_dataset/Products/things.csv", names=["name"])
pro_clothes = pd.read_csv("create_dataset/Products/clothes.csv", names=["name"])
pro_flowers = pd.read_csv("create_dataset/Products/flowers.csv", names=["name"])
pro_instru = pd.read_csv("create_dataset/Products/instruments.csv", names=['name'])

# preprocess the clothes data 
pro_clothes["product"] = pro_clothes["product"].apply(lambda x: str(x).strip())
pro_flowers["product"] = pro_flowers["product"].apply(lambda x: str(x).strip())
pro_instru["product"] = pro_instru["product"].apply(lambda x: str(x).strip())

# create categories
things = np.repeat("things",100)
clothes = np.repeat('clothes',61)
flowers = np.repeat('flowers',91)
instru = np.repeat('instru',52)

# adding the category
product["category"] = things
pro_clothes["category"] = clothes
pro_flowers["category"] = flowers
pro_instru['category'] = instru

# appending different types of products
product = product.append(pro_clothes)
product = product.append(pro_flowers)
product = product.append(pro_instru)

# resetting the index
product = product.reset_index(drop=True)

# create random numbers 
rand_ids = np.random.randint(1001,9999, (1,304))  
rand_ids = rand_ids.reshape(304,1)

# creating primary key 
product['product_id'] = rand_ids

# reordering and rearraning the columns
product = product.sort_values(['product_id'])
product = product[['product_id', 'category', 'product']]

# save the data
product.to_csv('data/product_list.csv', index=False)


# calling the mysql connector object
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="delhi_110062"
)
mycursor = mydb.cursor(buffered=True)
mycursor.execute("use eCommerce;")

# importing the product list
product = pd.read_csv("data/product_list.csv")

# insering the products into the database
for index, values in product.iterrows():
    sql = "INSERT INTO products (product_id, category, name ) VALUES (%s, %s, %s)"
    val = (values["product_id"], values["category"], values["name"])
    mycursor.execute(sql, val)

mydb.commit()

# mandatory to run this
mycursor.close()

