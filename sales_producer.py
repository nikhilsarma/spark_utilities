# -*- coding: utf-8 -*-
from kafka import KafkaProducer
from json import dumps
import pandas as pd
from time import sleep

KAFKA_SALES_TOPIC = 'order_sales'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

if __name__ == "__main__":

    print("kafka order_sales producer started...!")

    sales_producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, \
                                   value_serializer=lambda x: dumps(x).encode('utf-8'))

    #sales_path = r"C:\Users\SugarBox\Downloads\olist_orders_dataset.csv\olist_order_items_dataset.csv"
    sales_path = r'/home/nikil/Downloads/dsets/ecom/olist_order_items_dataset.csv'

    sales_pd_df = pd.read_csv(sales_path)
    sales_list = sales_pd_df.to_dict(orient="records")

    for sales_data in sales_list:
        message = sales_data
        print('message to be sent: ', message)
        sales_producer.send(KAFKA_SALES_TOPIC, message)
        sleep(2)
