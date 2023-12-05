# importing the libraries
from locale import currency
import mysql.connector
import time
import random

# setting up the mysql
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="delhi_110062"
)

mycursor = mydb.cursor(buffered=True)
mycursor.execute("use eCommerce;")
sql='''SELECT order_id
       FROM orders 
       WHERE order_id not in (SELECT order_id from completed_orders);
    ''' 

mycursor.execute(sql)
myresult = mycursor.fetchall()
i=len(myresult)

myresult_arr = [ele[0] for ele in myresult]
len(myresult_arr)

while i>0:
  curr_order_id_choice = random.choice(myresult_arr)
  print(curr_order_id_choice)
  myresult_arr.remove(curr_order_id_choice)
  update_sql="INSERT into completed_orders(order_id, completedAt) values(%s, CURRENT_TIMESTAMP) ;"
  val = (str(curr_order_id_choice),)
  mycursor.execute(update_sql,val)
  mydb.commit()
  i-=1
  time.sleep(5)
  
mycursor.close()


