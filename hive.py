import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('Persist')


def hive_persist(spark, df, dfname, partitionBy, mode):
    try:
        logger.warning("Saving the cities data to hive database...")
        spark.sql(""" Create database if not exists cities """)
        spark.sql("use cities")
        df.write.saveAsTable(dfname, partitionBy=partitionBy, mode=mode)

    except Exception as e:
        logger.error("An error has been occurred while saving dataframe to hive table city..")
        raise
    else:
        logger.warning("Successfully saved dataframe to hive table city..")


def hive_persist_prec(spark, df, dfname, partitionBy, mode):
    try:
        logger.warning("Saving the cities data to hive database...")
        spark.sql("Create database if not exists prescribers")
        spark.sql("use prescribers")
        df.write.saveAsTable(dfname, partitionBy=partitionBy, mode=mode)

    except Exception as e:
        logger.error("An error has been occurred while saving dataframe to hive table city..")
        raise
    else:
        logger.warning("Successfully saved dataframe to hive table city..")
