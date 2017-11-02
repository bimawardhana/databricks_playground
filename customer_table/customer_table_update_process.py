# Databricks notebook source
dbutils.widgets.text("unique_email_data_path", "")

# COMMAND ----------

# MAGIC %run "/marketing_tech/COMMON LIB/Read Google Sheet"

# COMMAND ----------

from pyspark.sql.functions import *

# Get product parameter (which product is run)
product = dbutils.widgets.get("product")

# Get configuration data for product that is run (data contains to which master data should new customer e-mail data is supposed to be appended to)
configs = get_gsheet_toSparkDF(os.path.join("/dbfs/mnt/S3_mktg_prod_general/mktg/bima_playground/", "drive_api.json"), "1a4fH78GI8KKCX7DiDJLra1h9w8SDg3GFTFMA9xeh2-Y", "Config")
configs = configs.filter(lower(col("product")) == product.lower())

# COMMAND ----------

import datetime

# Get today's date in YYYY-MM-DD format (used for filtering issued based on date)
run_from = datetime.datetime.now() - datetime.timedelta(days=1)
run_from_date = "{:02d}-{:02d}-{:02d}".format(last_week.year, last_week.month, last_week.day)

# UPDATE CUSTOMER TABLE BASED ON CONFIG
# Every config represent a configuration configuring to which customer table every new customer of a product is added to
for config in configs.toLocalIterator():
  
  # Get parameter's of current loop from config data
  product = config["product"]
  update_unique_email_to = config["update_unique_email_to"]
  
  # Load data
  issued = spark.read.parquet("/mnt/S3_mktg_prod_data/v1/raw/parquet/" + product + "/denormjs.track_" + product + "_issued.day_1/*/*/*")
  
  # Filter latest 7 days
  issued.createOrReplaceTempView("issued")
  issued = spark.sql("SELECT * FROM issued WHERE TO_DATE(FROM_UNIXTIME(CAST(issued.timestamp AS BIGINT)/1000 + (7 * 3600))) >= '" + last_week_date + "'")
  
  # Remove duplicate rows in issuance data
  issued.createOrReplaceTempView("issued")
  issued = spark.sql("SELECT *, ROW_NUMBER() OVER(PARTITION BY bookingId ORDER BY CASE WHEN cookieId IS NULL OR cookieId = '' THEN 1 ELSE 0 END) AS rnk FROM issued")
  issued = issued.filter(issued.rnk == 1)
  
  # Select only relevant column in issued data
  issued = issued.select("email", "timestamp", "bookingId")
  issued.createOrReplaceTempView("issued")
  
  # Load current customer master data
  current_customer_master_data = spark.read.parquet("/mnt/S3_mktg_prod_general/mktg/bima_playground/temp/customer_table/hotel_unique_email.pq/snapshot_date=2017-11-01") # DUMMY (should be from parquet)
  current_customer_master_data.createOrReplaceTempView("current_customer_master_data")
  
  if product.lower() == "affiliate":
    acquired_from_info = "'AFFILIATE_' || UPPER(affiliate_id)"
  else:
    acquired_from_info = "'TVLK_" + product.upper() + "'"
    
  updated_customer_master_data = spark.sql("""
                                               SELECT
                                                    -- Append contact email that has not been listed on customer master table
                                                    COALESCE(current_customer_master_data.contact_email, issued.email) AS contact_email,

                                                    -- Handling first_issued_timestamp and booking_id value (for each contact e-mail): 
                                                    -- 1) if contact e-mail has not been listed, 
                                                    -- 2) if contact-email has been listed but the listed timestamp is more recent than the issued one

                                                    -- First Issued Timestamp
                                                    CASE
                                                      WHEN current_customer_master_data.first_issued_timestamp IS NULL 
                                                        THEN issued.timestamp
                                                      WHEN issued.timestamp IS NULL 
                                                        THEN current_customer_master_data.first_issued_timestamp
                                                      WHEN current_customer_master_data.first_issued_timestamp <= issued.timestamp 
                                                        THEN current_customer_master_data.first_issued_timestamp
                                                      ELSE issued.timestamp
                                                    END AS first_issued_timestamp,

                                                    -- First Issued Booking ID
                                                    CASE
                                                      WHEN current_customer_master_data.booking_id IS NULL 
                                                        THEN issued.bookingId
                                                      WHEN issued.bookingId IS NULL 
                                                        THEN current_customer_master_data.booking_id
                                                      WHEN current_customer_master_data.booking_id <= issued.bookingId 
                                                        THEN current_customer_master_data.booking_id
                                                      ELSE current_customer_master_data.booking_id
                                                    END AS first_issued_booking_id,
                                                    
                                                    -- First Issued Acquired From
                                                    CASE
                                                      WHEN current_customer_master_data.first_issued_timestamp IS NULL 
                                                          THEN """ + acquired_from_info + """
                                                      WHEN issued.timestamp IS NULL 
                                                          THEN current_customer_master_data.acquired_from
                                                      WHEN current_customer_master_data.first_issued_timestamp <= issued.timestamp 
                                                          THEN current_customer_master_data.acquired_from
                                                      ELSE """ + acquired_from_info + """ 
                                                    END AS acquired_from 
                                                    
                                                FROM
                                                    current_customer_master_data FULL JOIN issued ON current_customer_master_data.contact_email = issued.email
                                           """)

# COMMAND ----------

# hotel_unique_email = spark.read.format("csv").option("header", True).load("/mnt/S3_mktg_prod_general/mktg/bima_playground/temp/customer_table/20171101.hotel_unique_email.csv")
# package_unique_email = spark.read.format("csv").option("header", True).load("/mnt/S3_mktg_prod_general/mktg/bima_playground/temp/customer_table/20171101.package_unique_email.csv")

# hotel_unique_email.write.parquet("/mnt/S3_mktg_prod_general/mktg/bima_playground/temp/customer_table/20171101.hotel_unique_email.pq")
# package_unique_email.write.parquet("/mnt/S3_mktg_prod_general/mktg/bima_playground/temp/customer_table/20171101.package_unique_email.pq")

# hotel_unique_email = spark.read.parquet("/mnt/S3_mktg_prod_general/mktg/bima_playground/temp/customer_table/20171101.hotel_unique_email.pq")
# package_unique_email = spark.read.parquet("/mnt/S3_mktg_prod_general/mktg/bima_playground/temp/customer_table/20171101.package_unique_email.pq")

# hotel_unique_email.createOrReplaceTempView("hotel_unique_email")
# package_unique_email.createOrReplaceTempView("package_unique_email")

# new_hotel_unique_email = spark.sql("""(SELECT *, "TVLK_PACKAGE" AS acquired_from FROM hotel_unique_email WHERE booking_id IN (SELECT booking_id FROM package_unique_email))
# UNION
# (SELECT *, "TVLK_HOTEL" AS acquired_from FROM hotel_unique_email WHERE booking_id NOT IN (SELECT booking_id FROM package_unique_email))""")

# new_hotel_unique_email.coalesce(4).write.parquet("/mnt/S3_mktg_prod_general/mktg/bima_playground/temp/customer_table/hotel_unique_email.pq/snapshot_date=2017-11-01")