import datetime
import time


def get_order_date(message) -> datetime.date:
        order_time = message["order_time"]
        order_time = datetime.datetime.strptime(order_time,"%m/%d/%Y-%H:%M:%S")
        return order_time.date()

def get_product_price(product:dict) -> int:
     return product["price"]*product["quantity"]

def get_total_order_price(order_details:list) -> int:
     total = 0
     for product in order_details:
          total += get_product_price(product)
     return total