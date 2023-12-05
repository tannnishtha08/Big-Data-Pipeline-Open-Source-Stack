import requests
import time

# this function will generate the batch data
def create_order():
    response = requests.get('http://127.0.0.1:5000/createorders')

# running the API
while(True):
    create_order()
    time.sleep(5)
    