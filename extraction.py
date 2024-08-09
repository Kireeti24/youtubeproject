import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')

loggers = logging.getLogger('Extraction')


def output(df, format, filepath, numPartitions, headerReq, compressionType):
    try:
        loggers.warning("Writing files to the output directory....")
        df.coalesce(numPartitions).write.mode('overwrite').format(format).save(filepath, header=headerReq,
                                                                               compression=compressionType)

    except Exception as e:
        loggers.error("An error has been occured while writing files to the output directory..", str(e))

        raise
    else:
        loggers.warning("Successfully written files to the output directory..")
