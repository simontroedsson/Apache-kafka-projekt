import tkinter as tk
from kafka import KafkaConsumer
import json
import datetime
from datetime import datetime as dt
from resources.get_functions import get_order_date,get_product_price

class ProcessedOrdersApp (object):
    def __init__(self):
        self.root=tk.Tk()
        self.consumer = KafkaConsumer(
        'processed_orders',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='group6',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_commit_interval_ms=1000,
        consumer_timeout_ms=100
        )
        self.root.geometry("1020x250")
        self.root.title("orderhantering consumer")
        self.order_dict = {}
        self.orders_last_5_min = 0
        self.orders_last_30_min = 0
        self.orders_last_60_min = 0
        self.orders_last_120_min = 0
        self.avg_orders_every_5_min_last_2_hours = 0
        date = dt.today()
        date_str = dt.strftime(date,"%Y-%m-%d %H:%M:%S")
        self.frame1=tk.Frame(self.root,background="black")
        self.date_label = tk.Label(self.frame1,text=date_str,font=("Arial",18),width=100,height=2)
        #self.date_label.pack(padx=40,pady=40)
        self.title_label = tk.Label(self.frame1,text="Processed orders",font=("Arial",18),height=2)
        #self.title_label.pack(padx=0,pady=0)
        self.frame1.columnconfigure(0,weight=1,pad=10)
        self.frame1.columnconfigure(1,weight=2,pad=10)
        self.date_label.grid(row=0,column=0,sticky=tk.W+tk.E, padx=1, pady=1)
        self.title_label.grid(row=1,column=0,sticky=tk.W+tk.E, padx=1, pady=1)
        self.frame1.pack()
        self.frame = tk.Frame(self.root,background="black")
        for col in range(5):
            self.frame.columnconfigure(col,weight=1,pad=30)
        self.col1_label = tk.Label(self.frame,text="last 5 min", font=("Arial",18))
        self.col2_label = tk.Label(self.frame,text="last 30 min", font=("Arial",18))
        self.col3_label = tk.Label(self.frame,text="last 60 min", font=("Arial",18))
        self.col4_label = tk.Label(self.frame,text="last 120 min", font=("Arial",18))
        self.col5_label = tk.Label(self.frame,text="Avg orders every 5 min last 2 hour", font=("Arial",18))
        self.processed_5_label = tk.Label(self.frame,text=self.orders_last_5_min,font=("Arial", 18),height=3)
        self.processed_30_label = tk.Label(self.frame,text=self.orders_last_30_min,font=("Arial", 18))
        self.processed_60_label = tk.Label(self.frame,text=self.orders_last_60_min,font=("Arial", 18))
        self.processed_120_label = tk.Label(self.frame,text=self.orders_last_120_min,font=("Arial", 18))
        self.processed_avg_5_label = tk.Label(self.frame,text=self.avg_orders_every_5_min_last_2_hours,font=("Arial", 18))
        self.col1_label.grid(row=0,column=0,sticky="nsew", padx=1, pady=1)
        self.col2_label.grid(row=0,column=1,sticky="nsew", padx=1, pady=1)
        self.col3_label.grid(row=0,column=2,sticky="nsew", padx=1, pady=1)
        self.col4_label.grid(row=0,column=3,sticky="nsew", padx=1, pady=1)
        self.col5_label.grid(row=0,column=4,sticky="nsew", padx=1, pady=1)
        self.processed_5_label.grid(row=1,column=0,sticky="nsew", padx=1, pady=1)
        self.processed_30_label.grid(row=1,column=1,sticky="nsew", padx=1, pady=1)
        self.processed_60_label.grid(row=1,column=2,sticky="nsew", padx=1, pady=1)
        self.processed_120_label.grid(row=1,column=3,sticky="nsew", padx=1, pady=1)
        self.processed_avg_5_label.grid(row=1,column=4,sticky="nsew", padx=1, pady=1)
        self.frame.pack(padx=0,pady=0)
        self.temp_date = datetime.date.today()
    def update_date(self):
        date = dt.today()
        date_str = dt.strftime(date,"%Y-%m-%d %H:%M:%S")
        self.date_label.config(text=date_str)
        self.root.after(1000,self.update_date)
    def update_data_labels(self):
        self.processed_5_label.config(text=self.orders_last_5_min)
        self.processed_30_label.config(text=self.orders_last_30_min)
        self.processed_60_label.config(text=self.orders_last_60_min)
        self.processed_120_label.config(text=self.orders_last_120_min)
        self.processed_avg_5_label.config(text=self.avg_orders_every_5_min_last_2_hours)
    def update_data(self):
        self.orders_last_5_min = 0
        self.orders_last_30_min = 0
        self.orders_last_60_min = 0
        self.orders_last_120_min = 0
        #Loop through all orders and se if their time lies
        #between the different time intervals
        for id,order_time in self.order_dict.items():
            if dt.now() >= order_time >= dt.now() - datetime.timedelta(minutes=5):
                self.orders_last_5_min+=1
            if dt.now() >= order_time >= dt.now() - datetime.timedelta(minutes=30):
                self.orders_last_30_min+=1
            if dt.now() >= order_time >= dt.now() - datetime.timedelta(minutes=60):
                self.orders_last_60_min+=1
            if dt.now() >= order_time >= dt.now() - datetime.timedelta(minutes=120):
                self.orders_last_120_min+=1
            else: # remove order from dict if its time is no longer in any of the intervals
                self.order_dict.pop(id)  
        self.avg_orders_every_5_min_last_2_hours = self.orders_last_120_min / 24 
        self.update_data_labels()

    def gather_data_from_topic(self):
        todays_date = datetime.date.today()
        if todays_date != self.temp_date:
            self.order_dict = {}     
        self.temp_date = todays_date
        for message in self.consumer:
            if datetime.date.today() == get_order_date(message.value):
                #Add incoming order and current time to order_dict
                order_id = message.value["order_id"]
                self.order_dict[order_id] = dt.now()
                self.update_data()
            break
        self.update_data()
        self.root.after(20,self.gather_data_from_topic)
    def run(self):
        self.root.after(20,self.gather_data_from_topic)
        self.root.after(1000,self.update_date)
        self.root.mainloop()

if __name__=='__main__':
    ProcessedOrdersApp().run()

