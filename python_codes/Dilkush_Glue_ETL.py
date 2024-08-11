import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import requests
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql import Row
import boto3
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Fetch Data from the API
url = 'https://dilkhush-api.onrender.com/orders'
response = requests.get(url)

if response.status_code == 200:
    data = response.json()

    # Step 2: Extract relevant information and organize it into a list of dictionaries
    orders_data = []
    for order in data:
        order_id = order['order_id']
        customer = order['customer']
        order_details = order['order_details']

        for item in order_details['items']:
            orders_data.append({
                'order_id': order_id,
                'customer_full_name': customer['full_name'],
                'customer_mobile_no': customer['mobile_no'],
                'customer_city': customer['city'],
                'customer_state': customer['state'],
                'customer_address': customer['address'],
                'customer_email': customer['email'],
                'item_name': item['item_name'],
                'category': item['category'],
                'quantity': item['quantity'],
                'price': item['price'],
                'total_price': order_details['total_price'],
                'mode_of_payment': order_details['mode_of_payment'],
                'purchase_time': order_details['purchase_time'],
                'day_part': order_details['day_part'],
            })

    # Step 3: Convert list of dictionaries to Spark DataFrame
    spark_df = spark.createDataFrame([Row(**i) for i in orders_data])

    # Step 4: Data Cleaning
    spark_df = spark_df.withColumn('purchase_time', spark_df['purchase_time'].cast('timestamp'))

    # Step 5: Coalesce into a single partition and write to S3 in CSV format
    output_path_temp = 's3://dilkhush-s3-bucket/DilKhush_Orders_Data/DilKhush_Raw_Data/temp_output'
    spark_df.coalesce(1).write.mode('overwrite').option("header", "true").csv(output_path_temp)

    # Step 6: Generate a timestamp-based file name
    s3 = boto3.client('s3')
    bucket = 'dilkhush-s3-bucket'
    prefix = 'DilKhush_Orders_Data/DilKhush_Raw_Data/'

    # Generate the timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    target_file_name = f'DilKhush_Raw_data_{timestamp}.csv'

    # Get the file name of the output
    result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix + 'temp_output/')
    for obj in result['Contents']:
        if obj['Key'].endswith('.csv'):
            source_key = obj['Key']
            break

    # Copy to the final location with the timestamped name
    copy_source = {'Bucket': bucket, 'Key': source_key}
    destination_key = prefix + target_file_name
    s3.copy_object(CopySource=copy_source, Bucket=bucket, Key=destination_key)

    # Delete the temporary folder
    s3.delete_object(Bucket=bucket, Key=source_key)

else:
    print(f"Failed to retrieve data: {response.status_code}")

job.commit()
