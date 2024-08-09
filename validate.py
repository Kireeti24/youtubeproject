import logging.config

from pyspark.sql.functions import *
logging.config.fileConfig('Properties/configuration/logging.config')

loggers = logging.getLogger('Validate')


def get_current_date(spark):
    try:
        loggers.warning('started the get_current_date method...')
        output = spark.sql("""select current_date""")
        loggers.warning("validating spark object with current date-" + str(output.collect()))

    except Exception as e:
        loggers.error('An error occured in get_current_date', str(e))

        raise

    else:
        loggers.warning('Validation done , go frwd...')