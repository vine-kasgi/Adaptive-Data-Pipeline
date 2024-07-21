import logging.config

logging.config.fileConfig('Properties/config/logging.config')

loggers = logging.getLogger('Validate')


def get_current_date(spark):
    try:
        loggers.warning('started the get_current_date method...')
        output = spark.sql('''select current_date''')
        loggers.warning('validating spark object with current date-' + str(output.collect()))

    except Exception as e:
        loggers.warning('An error occurred in get_current_date', str(e))

        raise

    else:
        loggers.warning('Validation done, go frwd...')
