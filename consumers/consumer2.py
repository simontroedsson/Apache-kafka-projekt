import tkinter as tk
from kafka import KafkaConsumer
import json
import datetime
from datetime import datetime as dt
from resources.get_functions import get_order_date,get_product_price

class SalesApp (object):
    def __init__(self):
        self.root=tk.Tk()
        self.consumer = KafkaConsumer(
        'Orders',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='group2',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_commit_interval_ms=1000,
        consumer_timeout_ms=100
        )
        self.root.geometry("500x250")
        self.root.title("Sales consumer")
        self.order_count=0
        date = dt.today()
        date_str = dt.strftime(date,"%Y-%m-%d %H:%M:%S")
        self.date_label = tk.Label(self.root,text=date_str,font=("Arial",18))
        self.date_label.pack(padx=40,pady=40)
        self.frame = tk.Frame(self.root)
        self.frame.columnconfigure(0,weight=1,pad=30)
        self.frame.columnconfigure(1,weight=2,pad=30)
        self.col1_label = tk.Label(self.frame,text="Total sales", font=("Arial",18))
        self.col2_label = tk.Label(self.frame,text="Sales this hour", font=("Arial",18))
        self.total_sales_label = tk.Label(self.frame,text=self.order_count,font=("Arial", 18))
        self.sales_last_hour_label = tk.Label(self.frame,text=self.order_count,font=("Arial", 18))
        self.col1_label.grid(row=0,column=0)
        self.col2_label.grid(row=0,column=1)
        self.total_sales_label.grid(row=1,column=0)
        self.sales_last_hour_label.grid(row=1,column=1)
        self.frame.pack(padx=0,pady=0)
        self.total_sales_today = 0
        self.total_sales_last_hour = 0
        self.total_sales_last_hour_time = dt.now().hour
        self.temp_date = datetime.date.today()
    def update_date(self):
        date = dt.today()
        date_str = dt.strftime(date,"%Y-%m-%d %H:%M:%S")
        self.date_label.config(text=date_str)
        self.root.after(1000,self.update_date)
    def update_sales_info(self):
        todays_date = datetime.date.today()
        if todays_date != self.temp_date:
            self.total_sales_today = 0
            self.temp_date = todays_date
            self.total_sales_label.config(text=self.total_sales_today)      
        self.temp_date = todays_date
        if self.total_sales_last_hour_time != dt.now().hour:
            self.total_sales_last_hour_time = dt.now().hour
            self.total_sales_last_hour = 0
            self.sales_last_hour_label.config(text=self.total_sales_last_hour)
        for message in self.consumer:
            if datetime.date.today() == get_order_date(message.value) :
                for product in message.value["order_details"]:
                    product_price = get_product_price(product)
                    self.total_sales_today += product_price
                    self.total_sales_last_hour += product_price
                    #Update labels
                    self.total_sales_label.config(text=self.total_sales_today)
                    self.sales_last_hour_label.config(text=self.total_sales_last_hour)
            break
        self.root.after(20,self.update_sales_info)
    def run(self):
        self.root.after(20,self.update_sales_info)
        self.root.after(1000,self.update_date)
        self.root.mainloop()

if __name__=='__main__':
    SalesApp().run()

