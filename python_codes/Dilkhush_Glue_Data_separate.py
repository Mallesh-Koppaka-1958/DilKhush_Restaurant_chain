import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Orders_Raw S3
Orders_RawS3_node1722960071771 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://dilkhush-s3-bucket/DilKhush_Orders_Data/DilKhush_Raw_Data/"]}, transformation_ctx="Orders_RawS3_node1722960071771")

# Script generated for node Order's Query
SqlQuery0 = '''
select  order_id,item_name,quantity,price,total_price,mode_of_payment,purchase_time,day_part from myDataSource
'''
OrdersQuery_node1722962983374 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Orders_RawS3_node1722960071771}, transformation_ctx = "OrdersQuery_node1722962983374")

# Script generated for node Items Query
SqlQuery1 = '''
select distinct item_name,price,price/quantity as Item_Price,category from myDataSource
'''
ItemsQuery_node1722962319115 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":Orders_RawS3_node1722960071771}, transformation_ctx = "ItemsQuery_node1722962319115")

# Script generated for node Customers Query
SqlQuery2 = '''
select distinct order_id,customer_full_name,customer_mobile_no,customer_city,customer_state,customer_address,customer_email from myDataSource
'''
CustomersQuery_node1722962765526 = sparkSqlQuery(glueContext, query = SqlQuery2, mapping = {"myDataSource":Orders_RawS3_node1722960071771}, transformation_ctx = "CustomersQuery_node1722962765526")

# Script generated for node Orders S3
OrdersS3_node1722963248632 = glueContext.write_dynamic_frame.from_options(frame=OrdersQuery_node1722962983374, connection_type="s3", format="csv", connection_options={"path": "s3://dilkhush-s3-bucket/DilKhush_Orders_Data/Orders_details/", "partitionKeys": []}, transformation_ctx="OrdersS3_node1722963248632")

# Script generated for node Items S3
ItemsS3_node1722962722438 = glueContext.write_dynamic_frame.from_options(frame=ItemsQuery_node1722962319115, connection_type="s3", format="csv", connection_options={"path": "s3://dilkhush-s3-bucket/DilKhush_Orders_Data/Items_details/", "partitionKeys": []}, transformation_ctx="ItemsS3_node1722962722438")

# Script generated for node Customer S3
CustomerS3_node1722962911331 = glueContext.write_dynamic_frame.from_options(frame=CustomersQuery_node1722962765526, connection_type="s3", format="csv", connection_options={"path": "s3://dilkhush-s3-bucket/DilKhush_Orders_Data/customer_details/", "partitionKeys": []}, transformation_ctx="CustomerS3_node1722962911331")

job.commit()
