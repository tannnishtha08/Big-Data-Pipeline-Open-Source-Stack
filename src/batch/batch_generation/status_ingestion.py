import requests
import time

def update_order():
    response = requests.get('http://127.0.0.1:5000/updateorders')

if __name__== '__main__':
    while True:
        update_order()
        