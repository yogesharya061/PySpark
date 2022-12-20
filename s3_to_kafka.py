from pyspark.sql import SparkSession
import sys

def send_s3_to_kafka(spark, params):
    """
        This function is responsible for reading data from s3 and push the data to Kafka topic in JSON format.
        It will check for NULL value before sending data and make it "". otherwise for NULL values, spark will not create column at all.
        params:
            s3_url: s3 file path location from where files needs to be read and processed.
            kafka_server: Details of kafka host and port.
            key: Key column for the data which acts as a unique key for Kafka topic.
            topic: Name of kafka topic in which data needs to push.
            drop_cols: Optional parameter. List of column names which needs to be dropped before sending.
        return:
            Data pushed to Kafka topic.
    """
    from pyspark.sql.functions import to_json, struct, col
    s3_dest = params['s3_url']
    server = params['kafka_server']
    key_column = params['key']
    topic = params['topic']

    src_df = spark.read.parquet(s3_dest)
    try:
        drop_col = params['drop_cols']
        src_df = src_df.drop(*drop_col)
    except KeyError as e:
        logger.info(f"No columns need to be dropped before data send: {e}")

    src_df.createOrReplaceTempView('SRC_DF')

    logger.info(f"*** Format the updates for Kafka from {s3_dest}")
    column_dtl = src_df.dtypes

    sqlcmd = "select "
    for column in column_dtl:
        if column[1] == 'timestamp' or column[1] == 'date':
            sqlcmd += "nvl(date_format(" + column[0] + ",\"yMMdd\")||'T'||date_format(" + column[0] + \
                      ",\"HHmmss\")||'.000 GMT',\"\") as " + column[0] + ","
        else:
            sqlcmd += "nvl(" + column[0] + ", \"\") as " + column[0] + ","

    cmd = sqlcmd[:-1] + " from SRC_DF"
    logger.info(f"SQL Command: {cmd}")
    df = spark.sql("{}".format(cmd))

    df_json = df.select([col(key_column).alias('key'),
                         to_json(struct([df[f] for f in df.columns])).alias('value')])
    df_json.write.format("kafka").option("kafka.bootstrap.servers", server) \
        .option("topic", topic).save()


def main():
    # Reading command line arguments for kafka process
    params = {
        "s3_url": sys.argv[1],
        "kafka_server": sys.argv[2],
        "key": sys.argv[3],
        "topic": sys.argv[4],
    }

    spark = SparkSession.builder \
      .master("local[1]") \
      .appName("s3_to_kafka") \
      .getOrCreate() 
    
    send_s3_to_kafka(spark, params)
  
    # Stopping spark session
    spark.stop()
    return None


# entry point for PySpark application
if __name__ == '__main__':
    main()

# spark-submit --master local[*] --packages 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0' s3_to_kafka.py "s3a://my_bucket/test/" "kafka_host:kafka_port" "test-topic" "id"
