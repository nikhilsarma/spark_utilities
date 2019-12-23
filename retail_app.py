from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import time


#define schema for sales and order topic data that is being consumed by the stream
salesSchema = StructType() \
    .add("order_id", StringType()) \
    .add("order_item_id", StringType()) \
    .add("product_id", StringType()) \
    .add("seller_id", StringType()) \
    .add("shipping_limit_date", TimestampType()) \
    .add("price", FloatType()) \
    .add("freight_value", FloatType())

statusSchema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("order_status", StringType()) \
    .add("order_purchase_timestamp", TimestampType()) \
    .add("order_approved_at", TimestampType()) \
    .add("order_delivered_carrier_date", TimestampType()) \
    .add("order_delivered_customer_date", TimestampType()) \
    .add("order_estimated_delivery_date", TimestampType())

#

def save_cust_data_mysql(data, epoc_id):
    print(f'saving customer data to mysql! batch: {epoc_id}')
    #dataframe column order doesnot matter, save will happen accordingly to column name:value
    cols_to_select = ['order_id','customer_unique_id', 'order_status', 'order_purchase_timestamp', 'customer_city', 'customer_state', 'topic']
    processed_at = time.strftime("%Y-%m-%d %H:%M:%S")
    mysqldatatodb = data.select(cols_to_select).withColumn("processed_at", F.lit(processed_at)).withColumn("batch_id", F.lit(epoc_id))

    mysqldatatodb.write.format('jdbc') \
        .option("url", 'jdbc:mysql://localhost/sales') \
        .option("dbtable", 'customer_orders') \
        .options(user="admin", password="123456")\
        .mode('append').save()

    print("finished saving data mysql")

def save_prod_data_mongo(data, epoc_id):

    print(f'saving product sales data to mongodb! batch: {epoc_id}')

    processed_at = time.strftime("%Y-%m-%d %H:%M:%S")
    proddatatodb = data.withColumn("processed_at", F.lit(processed_at)).withColumn("batch_id", F.lit(epoc_id))

    proddatatodb.write.format("mongo") \
        .option("spark.mongodb.output.uri", "mongodb://localhost/sales.product_data") \
        .mode("append").save()

    print("finished saving the product data")

def save_seller_data_mongo(data, epoc_id):
    print(f'saving seller sales data to mongodb! batch: {epoc_id}')

    processed_at = time.strftime("%Y-%m-%d %H:%M:%S")
    sellerdatatodb = data.withColumn("processed_at", F.lit(processed_at)).withColumn("batch_id", F.lit(epoc_id))

    sellerdatatodb.write.format("mongo") \
        .option("spark.mongodb.output.uri", "mongodb://localhost/sales.seller_data") \
        .mode("append").save()

    print("finished saving the seller data")




"""
mysqldf = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost/sales") \
    .option("dbtable", "customer_orders")\
    .options(user="admin", password="123456")\
    .load()

print ("reading data", mysqldf.show())

mongodf = spark.read.format("mongo") \
                    .option("spark.mongodb.input.uri", "mongodb://localhost/nikhil.testusers") \
                    .option("spark.mongodb.output.uri","mongodb://localhost/nikhil.testusers") \
                    .load()
"""
#Read data from the mysql database
#.option("driver", "com.mysql.cj.jdbc.Driver")
#Loading class `com.mysql.jdbc.Driver'. This is deprecated. The new driver class is `com.mysql.cj.jdbc.Driver'. The driver is automatically registered via the SPI and manual loading of the driver class is generally unnecessary.
"""
mysqldf = spark.read.format("jdbc") \
                    .option("url", "jdbc:mysql://localhost/sales") \
                    .option("dbtable", "sales_data") \
                    .options(user="admin", password="123456") \
                    .load()
#aa = mysqldf.select('source').first()
print(mysqldf.show())
"""


if __name__ == "__main__":

    print("structured Stream app started!!")

    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic_names = "order_sales, order_status"

    customer_path = r'/home/nikil/Downloads/dsets/ecom/olist_customers_dataset.csv'
    product_path = r'/home/nikil/Downloads/dsets/ecom/olist_products_dataset.csv'
    seller_path = r'/home/nikil/Downloads/dsets/ecom/olist_sellers_dataset.csv'
    translate_path = r'/home/nikil/Downloads/dsets/ecom/product_category_name_translation.csv'

    spark = SparkSession.builder.appName("myApp").master("local[*]").getOrCreate()
    print("Spark Session started!")

    spark.sparkContext.setLogLevel("ERROR")
    current_conf_params = spark.sparkContext.getConf().getAll()

    # loading the csv files into respective dataframes

    dfproduct = spark.read.csv(product_path, inferSchema=True, header=True)
    dfcustomer = spark.read.csv(customer_path, inferSchema=True, header=True)
    dfseller = spark.read.csv(seller_path, inferSchema=True, header=True)
    dftrans = spark.read.csv(translate_path, inferSchema=True, header=True)

    # aliasing dataframes (to ease out unambigous column_names after joining)
    tproduct = dfproduct.alias('tproduct')
    tcustomer = dfcustomer.alias('tcustomer')

    # joining product description table with product_names_translator table (portugese to english)
    prod_desc_cols = ['product_id', 'product_category_name', 'product_photos_qty']
    dfproduct_desc = dfproduct.select(prod_desc_cols).join(dftrans, ['product_category_name'], 'left')
    tproduct_desc = dfproduct_desc.alias('tproduct_desc')
    tseller = dfseller.alias('tseller')

    #read the data_stream from a kafka topic
    data_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("subscribe", kafka_topic_names) \
        .option("startingOffsets", "latest") \
        .load()

    #load the kafka message value as string into local dataframe along with the timestamp of kafka processed time.
    #this stream holds data from multiple_topics (as we subscribed to multiple_topics)
    data_stream_local = data_stream.selectExpr("CAST(value as STRING)", "topic", "timestamp")

    #sales and status data computed.
    #string value from the kafka message value converted to --> predefined Schema
    # Also filterout the messages from mainStream based on topic_name 
    salesdata = data_stream_local.select(F.from_json('value', salesSchema).alias("sales"), "timestamp", "topic") \
        .where("topic == 'order_sales'")
    statusdata = data_stream_local.select(F.from_json('value', statusSchema).alias("status"), "timestamp", "topic") \
        .where("topic == 'order_status'")

    #expand the sales, status data into full column names sales.*/status.*)
    salesdatatemp = salesdata.select("sales.*", "topic", "timestamp")
    statusdatatemp = statusdata.select("status.*", "topic", "timestamp")

    #join the customer_order(static) table to the statusdatatemp stream dataFrame
    #join the product_description(static) table to the salesdatatemp stream dataFrame

    """
    TO-DO
    Broadcast static tables.
    instead joining the customer_order, product_desc tables for each batch of incoming streams

    """
    cust_order_data = statusdatatemp.join(tcustomer, ['customer_id'], 'left')
    prod_sales_data = salesdatatemp.join(tproduct_desc, ['product_id'], 'left')
    seller_sales_data = prod_sales_data.join(tseller, ['seller_id'], 'left')

    cust_order_data.writeStream.trigger(processingTime='10 seconds').outputMode('update').foreachBatch(
        save_cust_data_mysql).start()
    prod_sales_data.writeStream.trigger(processingTime='20 seconds').outputMode('update').foreachBatch(
        save_prod_data_mongo).start()
    seller_sales_data.writeStream.trigger(processingTime='20 seconds').outputMode('update').foreachBatch(
        save_seller_data_mongo).start()

    seller_console_stream= seller_sales_data.groupby('seller_city') \
        .agg(
        F.round(F.sum('price'), 2).alias('total_price'),
        F.round(F.sum('freight_value'), 2).alias('dispatch_price'),
        F.count('order_id').alias('items_sold')
    )

    seller_city_group_stream = seller_console_stream.writeStream.trigger(processingTime='30 seconds') \
        .outputMode('update').option("truncate", 'false') \
        .format("console").start()

    seller_city_group_stream.awaitTermination()

    print ("finished processing the data")