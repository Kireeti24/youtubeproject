from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('Data_transformation')


def transform_data(df1, df2):
    try:
        logger.warning('Transforming data of df1', str(df1.columns))
        df1 = df1.withColumn('zipcode_count', size(split('zips', ' ')))

        logger.warning('Transforming data of df2', str(df2.columns))

        df2 = df2.filter(col('years_of_exp').between(20, 50))

        window_spec = Window.partitionBy('presc_state').orderBy(col("total_claim_count").desc())

        df2 = df2.withColumn('Rank', dense_rank().over(window_spec)).filter(col('Rank') <= 5)



    except Exception as e:
        logger.error("An error has been occurred while transforming data", str(e))
        raise

    else:
        logger.info("Data transformed successfully")
    return df1, df2
