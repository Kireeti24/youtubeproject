from pyspark.sql import SparkSession

import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('Create_spark')


def get_spark_object(enrv, appName):
    try:
        logger.info('get_spark_object method started')

        if enrv == 'DEV':
            master = 'local'

        else:
            master = 'Yarn'

        logger.info('master is {}'.format(master))

        spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()

    except Exception as exp:
        logger.error('An error occurred in the get_spark_object==== ', str(exp))
        raise

    else:
        logger.info('Spark object created.....')
    return spark
