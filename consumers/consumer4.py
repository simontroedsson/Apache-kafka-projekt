import tkinter as tk
from kafka import KafkaConsumer
import json
import sqlite3

class InventoryBalanceApp (object):
    def __init__(self):
        self.root=tk.Tk()
        self.consumer = KafkaConsumer(
        'Orders',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='group4',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_commit_interval_ms=1000,
        consumer_timeout_ms=100
        )
        self.conn = sqlite3.connect('resources/projekt.db') 
        self.cur = self.conn.cursor() 
        self.root.geometry("300x845")
        self.root.title("Inventory_balance")
        self.frame = tk.Frame(self.root,background="black")
        table = self.cur.execute("""SELECT product_name, Units_in_stock
        FROM product""").fetchall()
        columns = 2
        self.widgets = []
        self.col1_name = tk.Label(self.frame,text="Product name",font=("Arial",16))
        self.col2_name = tk.Label(self.frame,text="Units in stock",font=("Arial",16))
        self.col1_name.grid(row=0,column=0,sticky="nsew",padx=1,pady=1)
        self.col2_name.grid(row=0,column=1,sticky="nsew",padx=1,pady=1)
        i=1
        for (product_name,units_in_stock) in table:
            label_col1 = tk.Label(self.frame,text=product_name,font=("Arial",16),borderwidth=0)
            label_col2 = tk.Label(self.frame,text=units_in_stock,font=("Arial",16),borderwidth=0)
            label_col1.grid(row=i,column=0,sticky="nsew", padx=1, pady=1)
            label_col2.grid(row=i,column=1,sticky="nsew", padx=1, pady=1)
            self.widgets.append([label_col1,label_col2])
            i+=1
        for column in range(columns):
            self.frame.grid_columnconfigure(column, weight=1,pad=20)
        self.frame.pack(padx=0,pady=0)

    def update_table(self):
        table = self.cur.execute("""SELECT product_name, Units_in_stock
        FROM product""").fetchall()
        i=0
        for product_name,units_in_stock in table:
            self.set_widget(i,0,product_name)
            self.set_widget(i,1,units_in_stock)
            i+=1
    def set_widget(self,row,column,value):
        widget = self.widgets[row][column]
        widget.configure(text=value)
    def gather_data_from_topic(self):
        for message in self.consumer:
            for product in message.value["order_details"]:
                product_id = product["product_id"]
                (units_in_stock,) = self.cur.execute("""SELECT units_in_stock 
                                                FROM product
                                                WHERE product_id = ?""",(product_id,)).fetchone()
                quantity_sold = product["quantity"]
                if units_in_stock >= quantity_sold:
                    units_in_stock = units_in_stock - quantity_sold
                    self.cur.execute("""UPDATE product
                                SET units_in_stock = ?
                                WHERE product_id = ?""",(units_in_stock,product_id))
                    self.conn.commit()
                    self.update_table()
            break 
        self.root.after(100,self.gather_data_from_topic)
    def run(self):
        self.root.after(100,self.gather_data_from_topic)
        self.root.mainloop()

if __name__=='__main__':
    InventoryBalanceApp().run()

