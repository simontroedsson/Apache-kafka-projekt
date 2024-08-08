import tkinter as tk
from kafka import KafkaConsumer
import json
import datetime
from resources.get_functions import get_order_date

class App (object):
    def __init__(self):
        self.root=tk.Tk()
        self.consumer = KafkaConsumer(
        'Orders',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='group1',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_commit_interval_ms=1000,
        consumer_timeout_ms=100
        )
        self.root.geometry("250x250")
        self.root.title("Antal ordrar")
        self.order_count=0
        self.frame = tk.Frame(self.root)
        self.frame.columnconfigure(0,weight=1)
        self.frame.columnconfigure(1,weight=2)
        self.label1 = tk.Label(self.frame,text="Antal ordrar", font=("Arial",18))
        self.label2 = tk.Label(self.frame,text=self.order_count,font=("Arial", 18))
        self.label1.grid(row=0,column=0)
        self.label2.grid(row=1,column=0,pady=20)
        #self.label1.pack(padx=40,pady=40)
        #self.label2.pack(padx=0,pady=0)
        self.frame.pack(padx=50,pady=50)
        self.temp_date = datetime.date.today()
        
    def update_order_count(self):
        todays_date = datetime.date.today()
        if todays_date != self.temp_date:
            self.order_count = 0
            self.temp_date = todays_date      
        self.temp_date = todays_date
        for message in self.consumer:
            if todays_date == get_order_date(message.value):
                self.order_count=self.order_count+1
                self.label2.config(text=self.order_count)
            break
        self.root.after(20,self.update_order_count)
    def run(self):
        self.root.after(20,self.update_order_count)
        self.root.mainloop()

if __name__=='__main__':
    App().run()

