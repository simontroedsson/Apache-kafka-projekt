from kafka import KafkaConsumer,KafkaProducer
import json
import datetime
import time
from datetime import datetime as dt
import random
from resources.get_functions import get_order_date,get_product_price,get_total_order_price
PERCENT_CHANCE = 0.20

def send_confirmation_to_customer(message:dict):
     with open('order_confirmation.txt', 'a') as file:
          order_id = message["order_id"]
          order_time = message["order_time"]
          file.write("--------------------------------------\n")
          file.write("      Thank you for your order!\n")
          file.write("your order information is listed below\n")
          file.write("--------------------------------------\n")
          file.write("\tOrderdetails:\n")
          file.write(f"\torderid:{order_id}\n")
          file.write(f"\tordertime:{order_time}\n")
          file.write("--------------------------------------\n")
          file.write("\tProduct"+"Qty".rjust(13)+"Price\n".rjust(13))
          file.write("--------------------------------------\n")
          for product in message["order_details"]:
               str_length = len(product["product_name"])
               rljust_lenght = 20 - str_length
               product_name = product["product_name"]
               product_quantity = product["quantity"]
               file.write(f"\t{product_name}"+f"{product_quantity}".rjust(rljust_lenght)+f"{get_product_price(product)}\n".rjust(13))
          file.write("--------------------------------------\n")
          file.write("\tTotal"+f"{get_total_order_price(message['order_details'])}".rjust(27))
          file.write("\n")
          file.write("\n")

if __name__ == "__main__":
    consumer = KafkaConsumer(
        'Orders',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='group5',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_commit_interval_ms=3000,
        consumer_timeout_ms=500
    )
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    try:
        while True:
            for message in consumer:
                todays_date = datetime.date.today()
                order_date = get_order_date(message.value)
                if random.random() < PERCENT_CHANCE and todays_date == order_date:
                    order_id = message.value["order_id"]
                    print(f"Order_id:{order_id}Proccessed...")
                    producer.send('processed_orders',message.value)
                    producer.flush()
                    send_confirmation_to_customer(message.value)
            print("idle...")     
            time.sleep(1)           
                
                    
    except KeyboardInterrupt:
        print('Closing')
    finally:
        consumer.close()