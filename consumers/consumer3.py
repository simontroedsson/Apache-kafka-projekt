import tkinter as tk
from kafka import KafkaConsumer
import json
import datetime as dt
from resources.get_functions import get_product_price,get_order_date

class DailyReportApp (object):
    def __init__(self):
        self.root=tk.Tk()
        self.consumer = KafkaConsumer(
        'Orders',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='group3',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_commit_interval_ms=1000,
        consumer_timeout_ms=100
        )
        self.root.geometry("400x200")
        self.root.title("Daily report consumer")
        date = dt.datetime.today()
        date_str = dt.datetime.strftime(date,"%Y-%m-%d %H:%M:%S")
        self.frame = tk.Frame(self.root)
        self.frame.columnconfigure(0,weight=1,pad=30)
        self.frame.columnconfigure(1,weight=2,pad=30)
        self.date_label = tk.Label(self.frame,text=date_str,font=("Arial",18))
        self.daily_report_button = tk.Button(self.frame,text="Write daily report",font=("Arial",18),command=self.write_report_to_file)
        self.date_label.grid(row=0,column=0)
        self.daily_report_button.grid(row=1,column=0)
        self.frame.pack(padx=40,pady=40)
        self.order_count_today = 0
        self.total_sales_today = 0
        self.products_sold = {}
        self.temp_date = dt.date.today()
    def write_report_to_file(self):
        with open('daily_reports.json','a') as json_file:
            products_sold_list = []
            for product_name,quantity_sold in self.products_sold.items():
                products_sold_list.append({"product_name":product_name,"quantity_sold": quantity_sold})

            dt_str = dt.datetime.strftime(self.temp_date,"%Y-%m-%d")
            data = dict(date = dt_str,
                        order_count=self.order_count_today,
                        total_sales=self.total_sales_today,
                        products_sold=products_sold_list)
            json.dump(data, json_file, indent=4)
    def update_date(self):
        date = dt.datetime.today()
        date_str = dt.datetime.strftime(date,"%Y-%m-%d %H:%M:%S")
        self.date_label.config(text=date_str)
        self.root.after(1000,self.update_date)
    def gather_data_from_topic(self):
        todays_date = dt.date.today()
        if todays_date != self.temp_date:
            self.write_report_to_file()  
            self.total_sales_today = 0
            self.order_count_today = 0 
            self.products_sold = {}   
        self.temp_date = todays_date
        for message in self.consumer:
            if todays_date == get_order_date(message.value):
                for product in message.value["order_details"]:
                    self.total_sales_today += get_product_price(product)
                    self.order_count_today +=1
                    product_name = product["product_name"]
                    quantity_sold = product["quantity"]
                    if product_name not in self.products_sold:
                        self.products_sold[product_name] = quantity_sold   
                    else:
                        self.products_sold[product_name] += quantity_sold 
            break 
        self.root.after(300,self.gather_data_from_topic)
    def run(self):
        self.root.after(300,self.gather_data_from_topic)
        self.root.after(1000,self.update_date)
        self.root.mainloop()

if __name__=='__main__':
    DailyReportApp().run()

