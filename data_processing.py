import logging.config

from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.config.fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger('Data_processing')


def clean(df1, df2):
    try:
        logger.warning('Cleaning data method started')

        df1 = df1.select(upper(col('city')).alias('city'), upper(col('state_id')).alias('state_id'),
                         upper(col('state_name')).alias("state_name"), upper('county_name').alias('county_name'),
                         col('population'), 'zips')
        df1.show()

        df2 = df2.select(col('npi').alias('presc_id'), col('nppes_provider_last_org_name').alias('presc_lname'),
                         col('nppes_provider_first_name').alias('presc_fname'),
                         col('nppes_provider_city').alias('city'), col('nppes_provider_state').alias('presc_state'),
                         col('specialty_description').alias('presc_spclty'), col('total_claim_count'), col('drug_name'),
                         col('total_drug_cost'),
                         col('years_of_exp'), lit('USA').alias('country'))

        df2 = df2.withColumn('years_of_exp', regexp_replace(col('years_of_exp'), r"[=]", " ").cast('int')).withColumn(
            'presc_lname', concat(col('presc_fname'), lit(' '), col('presc_lname'))).withColumnRenamed('presc_lname',
                                                                                                       'Full_name').drop(
            'presc_fname')
        df2.show()

        df3 = df1.select([count(when(col(c).isNull(), c)).alias(c) for c in df1.columns])
        df3.show()
        df4 = df2.select([count(when(col(c).isNull(), c)).alias(c) for c in df2.columns])
        df4.show()

        df1 = df1.dropna()
        df1.show()
        df2 = df2.dropna()
        df2.show()


    except Exception as e:
        logger.error("An error has occured while cleaning data method", str(e))
        raise
    else:
        logger.warning("Cleaning data method completed")

    return df1, df2
