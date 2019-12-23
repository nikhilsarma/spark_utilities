# -*- coding: utf-8 -*-

from kafka import KafkaProducer
from json import dumps
import pandas as pd
from time import sleep

KAFKA_STATUS_TOPIC = 'order_status'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

if __name__ == "__main__":

    print("kafka order_status producer started...!")

    order_producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,\
                                    value_serializer=lambda x: dumps(x).encode('utf-8'))

    #order_path = r"C:\Users\SugarBox\Downloads\olist_orders_dataset.csv\olist_orders_dataset.csv"
    order_path = r'/home/nikil/Downloads/dsets/ecom/olist_orders_dataset.csv'

    order_pd_df = pd.read_csv(order_path)
    order_list = order_pd_df.to_dict(orient="records")

    for order_data in order_list:
        message = order_data
        print('message to be sent: ', message)
        order_producer.send(KAFKA_STATUS_TOPIC, message)
        sleep(2)
