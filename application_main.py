import sys
from lib import RawDataSplit, Utils, ConfigReader
from pyspark.sql.functions import *

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]

    app_config = ConfigReader.get_app_config(job_run_env)
    print(app_config)

    print("Creating Spark Session")
    spark = Utils.get_spark_session(job_run_env)
    print("Spark session created")

    new_df = RawDataSplit.read_input(spark, job_run_env)
    RawDataSplit.split_raw_dataset(new_df)

# Change made to test feature1 branch
#
# Change for feature3
#
