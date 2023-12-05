''' Preprocessing and migrating data from data lake to data warehouse'''

# importing the libraries
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import split, col, monotonically_increasing_id, row_number
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# calling spark session
spark = SparkSession.builder.master("local").appName("hdfs_test").getOrCreate()

''' ORDERS COMPLETED CODE '''

# importing all the files from the data lake  
orders_completed_data=spark.read.text("hdfs://localhost:9000/ecomm_data/data_lake/completed_orders")

# filter all the data that have _airbyte in the rows
orders_completed_data = orders_completed_data.filter("value not like '%_airbyte%'")

# get the order_id and and completed_at
orders_completed = orders_completed_data.withColumn('order_id', split(orders_completed_data['value'], ',').getItem(2)).withColumn('completed_at', split(orders_completed_data['value'], ',').getItem(3))

# taking only order_id and completed_at timestamp
orders_completed = orders_completed.select(orders_completed.order_id, orders_completed.completed_at)

# further removing the unwanted string from the columns
orders_completed = orders_completed.withColumn('order_id', split(orders_completed['order_id'], ':').getItem(1)).withColumn('completed_at', split(orders_completed['completed_at'], '""').getItem(3))

# save the data to the warehouse
orders_completed.repartition(5).write.mode('overwrite').option('header','true').csv('hdfs://localhost:9000/ecomm_data/data_warehouse/completed_orders')

''' ORDER CREATED CODE '''

# importing all the files from the data lake  
orders_data=spark.read.text("hdfs://localhost:9000/ecomm_data/data_lake/orders")

# filter all the data that have _airbyte in the rows
orders_data = orders_data.filter("value not like '%_airbyte%'")

# get the order_id and and completed_at
orders = orders_data.withColumn('order_id', split(orders_data['value'], ',').getItem(2)).withColumn('user_id', split(orders_data['value'], ',').getItem(3)).withColumn('product_id', split(orders_data['value'], ',').getItem(4)).withColumn('ordered_at', split(orders_data['value'], ',').getItem(5))

# taking only order_id and completed_at timestamp
orders = orders.select(orders.order_id, orders.user_id, orders.product_id, orders.ordered_at)

# further removing the unwanted string from the columns
orders = orders.withColumn('order_id', split(orders['order_id'], ':').getItem(1)).withColumn('user_id', split(orders['user_id'], ':').getItem(1)).withColumn('product_id', split(orders['product_id'], ':').getItem(1)).withColumn('ordered_at', split(orders['ordered_at'], '""').getItem(3))

# save the data to the warehouse
orders.repartition(5).write.mode('overwrite').option('header','true').csv('hdfs://localhost:9000/ecomm_data/data_warehouse/orders')

