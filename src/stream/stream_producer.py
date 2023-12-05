# importing the libraries
import time
import requests
from kafka import KafkaProducer
import json

# kafka producer to stream random order -- order_view (when someone views their order)
def order_view():
    response = requests.get('http://127.0.0.1:5000/vieworders')
    # Parse the JSON response
    data = response.json()
    # Access the data in the response
    def json_serializer(data):
            return json.dumps(data).encode('utf-8')
    # Access the data in the response
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=json_serializer)
    # pushing the data to kafka stream
    try:
        producer.send("orderview",data)
    except Exception as e:
        print("Producer not working: ",e)

# kafka producer to stream random product views -- order_view (when someone views random products)
def product_view():
    response = requests.get('http://127.0.0.1:5000/viewproducts')
    # Parse the JSON response
    data = response.json()
    def json_serializer(data):
            return json.dumps(data).encode('utf-8')
    # Access the data in the response
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=json_serializer)
    # pusing the data to kafka stream
    try:
        producer.send("productview",data)
    except Exception as e:
        print("Producer not working: ",e)

i = 0
# running the APIs
while(True):
    product_view()
    if i%3 == 0:
        order_view()
    i+=1
    time.sleep(5)