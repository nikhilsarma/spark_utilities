# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 19:36:22 2019

@author: SugarBox
"""
from json import loads
from kafka import KafkaConsumer

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('order_sales',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest')
for message in consumer:
    print(message.timestamp)
    print(loads(message.value))

    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    # print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
    # message.offset, message.key,
    # message.value))

# consume earliest available messages, don't commit offsets
# KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)
