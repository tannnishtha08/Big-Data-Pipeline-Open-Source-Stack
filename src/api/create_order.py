# importing the libraries
import mysql.connector
from faker import Faker
import random
import time

# connecting to the sql
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="delhi_110062",
  database='eCommerce'
)

# loop to iterate and generate random orders at some interval 
#while(True):
uid = random.randint(1, 10000)
pid = random.randint(1, 304)
val=(uid,pid)
mycursor = mydb.cursor(buffered=True)
mycursor.execute("use eCommerce;")
# pusing to the database
sql="INSERT INTO orders(user_id,product_id, orderedAt) VALUES(%s,%s, CURRENT_TIMESTAMP);"
mycursor.execute(sql,val)
mydb.commit()
# mandatory to run this
mycursor.close()
  


