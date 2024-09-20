#Function to write the monitoring to s3
def write_monitoring_data(glueContext, monitoring_data):
    df = pd.DataFrame([monitoring_data])
    
    # Convert timestamps to ensure correct format
    df['initial_processing_timestamp'] = pd.to_datetime(df['initial_processing_timestamp'])
    df['final_processing_timestamp'] = pd.to_datetime(df['final_processing_timestamp'])
    
    # Define schema to ensure correct data types
    schema = StructType([
        StructField("Account_Name", StringType(), True),
        StructField("Account_id", StringType(), True),
        StructField("database", StringType(), True),
        StructField("table", StringType(), True),
        StructField("source_service", StringType(), True),
        StructField("job_name", StringType(), True),
        StructField("rows_processed", IntegerType(), True),
        StructField("initial_processing_timestamp", TimestampType(), True),
        StructField("final_processing_timestamp", TimestampType(), True),
        StructField("duration_sec", FloatType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True)
    ])
    
    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = glueContext.spark_session.createDataFrame(df, schema)
    dynamic_frame = DynamicFrame.fromDF(spark_df, glueContext, "MonitoringDF")
    
    # Write monitoring data to S3 in Parquet format
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": "s3://my-bucket"},
        format="parquet"
    )


#json format
monitoring_data = {
    "Account_Name": "account",  # AWS Account Name
    "Account_id" : number_account,  # AWS Account ID
    "database": "database-data-medallion-stage",  # Target database
    "table": "table_name",  # Target table
    "source_service": "service_name",  # Source service (Glue)
    "job_name": "job_name",  # Glue job name
    "rows_processed": rows_processed,  # Total rows processed
    "initial_processing_timestamp": start_time.isoformat(),  # Start timestamp
    "final_processing_timestamp": end_time.isoformat(),  # End timestamp
    "duration_sec": duration_sec,  # Job duration in seconds
    "status": status,  # Execution status (Success or Failed)
    "error_message": error_message if error_message else "",  # Error message, if any
}

