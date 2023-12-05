# importing the libraries
from flask import Flask, jsonify
import time
import subprocess
import mysql.connector
import random
import numpy

# connecting to the database
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="delhi_110062"
)
mycursor = mydb.cursor(buffered=True)
mycursor.execute("use eCommerce;")

# flask app
app = Flask(__name__)

# Defining route to create random orders
@app.route('/createorders', methods=['GET','POST'])
def run_script():
  script_path = r'src/api/create_order.py'
  subprocess.call(['python3', script_path])
  return 'Orders Created'

# Defining route to update random orders
@app.route('/updateorders', methods=['GET','POST'])
def updateorders():
  script_path = r'src/api/update_order.py'
  subprocess.call(['python3', script_path])
  return 'Orders Updated'

# Defining route to view random orders which are uncomplete(status=0)
@app.get('/vieworders')
def vieworders():
    sql="SELECT user_id,order_id FROM orders WHERE status='0' ORDER BY RAND() LIMIT 1;"
    mycursor.execute(sql)
    result=mycursor.fetchall()
    return [result[0][0], result[0][1]]

# Defining route to view random products
@app.get('/viewproducts')
def viewproducts():
    sql="SELECT product_id FROM products ORDER BY RAND() LIMIT 1;"
    mycursor.execute(sql)
    result=mycursor.fetchall()
    return [random.randint(1, 10000), result[0][0]]

if __name__ == '__main__':
  app.run()

# host="localhost", port=8000, debug=True

   




