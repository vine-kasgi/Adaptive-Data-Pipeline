import os
import sys

import get_env_variables as gav
from create_spark import get_spark_object
from validate import get_current_date
from ingest import load_files, display_df, df_count
import logging
import logging.config

logging.config.fileConfig('Properties/config/logging.config')


def main():
    global file_format, inferSchema, header, file_dir
    try:
        logging.info('i am in main method..')
        logging.info('calling spark object')

        spark = get_spark_object(gav.envn, gav.appName)

        logging.info('validating spark object..............')
        get_current_date(spark)

        # print(os.listdir(gav.src_olap))
        # print('-----------------------------------')
        # print('When multiple files are present in dir then....')

        # 1st for loop for OLAP

        for file in os.listdir(gav.src_olap):
            print('File is ' + file)

            file_dir = gav.src_olap + '\\' + file
            # print(file_dir)

            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
        logging.info('reading file which is of = {}'.format(file_format))

        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)
        logging.info('displaying the dataframe {}'.format(df_city))
        display_df(df_city, 'df_city')

        logging.info('validating the dataframe...')
        df_count(df_city, 'df_city')

        # 2nd for loop for OLTP

        for file2 in os.listdir(gav.src_oltp):
            print('File is ' + file2)

            file_dir = gav.src_oltp + '\\' + file2
            # print(file_dir)

            if file2.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif file2.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

        logging.info('reading file which is of = {}'.format(file_format))

        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)
        logging.info('displaying the dataframe {}'.format(df_fact))
        display_df(df_fact, 'df_fact')

        logging.info('validating the dataframe...')
        df_count(df_fact, 'df_fact')

    except Exception as exp:
        logging.error("An error occurred when calling main() please check the trace == ", str(exp))
        sys.exit(1)


if __name__ == '__main__':
    main()
    logging.info('Application done')
