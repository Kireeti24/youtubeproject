import os
import sys

from time import perf_counter

import get_env_properties as gav
from create_spark import get_spark_object
from validate import get_current_date
from ingest import load_files, display, validate_df
from data_processing import clean
from data_transformation import transform_data
from extraction import output
from hive import *
import logging
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')


def main():
    global file_format, file_dir, header, inferSchema, file_dir2, start_time
    try:
        start_time = perf_counter()
        logging.info('i am in the main method..')
        # print(gav.header)
        # print(gav.src_olap)
        logging.info('calling spark object')
        spark = get_spark_object(gav.enrv, gav.appName)

        logging.info('Validating spark object..........')
        get_current_date(spark)

        for file in os.listdir(gav.src_olap):
            file_dir = gav.src_olap + '\\' + file
            print(file_dir)
            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file.endswith('.csv'):
                file_format = 'csv'
                header = 'gav.header'
                inferSchema = 'gav.inferSchema'

        logging.info('reading file which is of type {}'.format(file_format))
        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)
        display(df_city, 'df_city')

        validate_df(df_city, 'df_city')

        for files in os.listdir(gav.src_oltp):
            file_dir = gav.src_oltp + '\\' + files
            print(file_dir)
            if files.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif files.endswith('.csv'):
                file_format = 'csv'
                header = 'gav.header'
                inferSchema = 'gav.inferSchema'

        logging.info('reading file which is of type {}'.format(file_format))
        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)
        display(df_fact, 'df_fact')
        validate_df(df_fact, 'df_fact')

        df_city.printSchema()
        df_fact.printSchema()

        logging.info("Implementing data Processing now....")

        df_city_sle, df_fact_sel = clean(df_city, df_fact)

        df_city_transf, df_fact_transf = transform_data(df_city_sle, df_fact_sel)
        display(df_city_transf, 'df_city_transf')
        display(df_fact_transf, 'df_fact_transf')

        logging.info("Saving data to local destination...")
        output(df_city_transf, 'orc', gav.city_path, 2, False, 'snappy')
        output(df_fact_transf, 'parquet', gav.prescriber, 2, False, 'snappy')

        logging.info("Implementing hive function now....")
        hive_persist(spark=spark, df=df_city_transf, dfname='df_city', partitionBy='state_name', mode='append')
        hive_persist_prec(spark=spark, df=df_fact_transf, dfname='df_presc', partitionBy='presc_state', mode='append')

    except Exception as exp:
        logging.error("An error occurred when calling main() please check the trace=== {}".format(str(exp)))
        sys.exit(1)


if __name__ == '__main__':
    main()
    end_time = perf_counter()
    print(f"Time taken to complete the application {end_time - start_time: 0.2f} seconds")
    logging.info('Application done with no errors')
