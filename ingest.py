import get_env_properties as gav
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('Ingest')


def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        if file_format == 'parquet':
            df = spark.read.parquet(file_dir)
        elif file_format == 'csv':
            df = spark.read.format(file_format) \
                .option('header', gav.header) \
                .option('inferSchema', gav.inferSchema) \
                .load(file_dir)
    except Exception as e:
        logger.error('An error occured while loading files: {}'.format(e))
        raise

    else:
        logger.warning('dataframe is created successfully which is of {}'.format(file_format))
    return df


def display(df, dfname):
    return df.show()


def validate_df(df, dfname):
    try:
        df_c = df.count()
    except Exception as e:
        raise
    else:
        logger.warning('Number of records present in the {} are :: {}'.format(df, df_c))

    return df_c
