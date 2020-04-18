#!/usr/bin/env python

__author__ = "Mageswaran Dhandapani"
__copyright__ = "Copyright 2020, The Spark Structured Playground Project"
__credits__ = []
__license__ = "Apache License"
__version__ = "2.0"
__maintainer__ = "Mageswaran Dhandapani"
__email__ = "mageswaran1989@gmail.com"
__status__ = "Education Purpose"

import gin
import argparse
import os
import pandas as pd
import numpy as np
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from pretty_print import print_error, print_info
from absl import flags
from absl import app

@gin.configurable
class PostgresqlConnection(object):
    """
    Postgresql utility class to read,write tables and execute query

    :param postgresql_host: Postgresql Host address
    :param postgresql_port: Postgresql port number
    :param postgresql_database: Postgresql database name
    :param postgresql_user: Postgresql user name
    :param postgresql_password: Postgresql password
    """
    def __init__(self,
                 postgresql_host="localhost",
                 postgresql_port="5432",
                 postgresql_database="taggerdb",
                 postgresql_user="tagger",
                 postgresql_password="tagger"):

        self._postgresql_host = postgresql_host
        self._postgresql_port = postgresql_port
        self._postgresql_database = postgresql_database
        self._postgresql_user = postgresql_user
        self._postgresql_password = postgresql_password

        self._db_url = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(self._postgresql_user,
                                                                     self._postgresql_password,
                                                                     self._postgresql_host,
                                                                     self._postgresql_port,
                                                                     self._postgresql_database)
        self._sqlalchemy_engine = None
        self._sqlalchemy_session = None
        self._sqlalchemy_connection = None

    def get_sqlalchemy_session(self):
        if self._sqlalchemy_session:
            return self._sqlalchemy_session

        if self._sqlalchemy_engine is None:
            self._sqlalchemy_engine = create_engine(self._db_url, pool_recycle=3600)

        session = sessionmaker(bind=self._sqlalchemy_engine)
        self._sqlalchemy_session = session()
        return self._sqlalchemy_session

    def get_sqlalchemy_connection(self):
        """
        :return: Returns postgresql sqlalchemy connection
        """
        if self._sqlalchemy_connection:
            return self._sqlalchemy_connection

        # Connect to database (Note: The package psychopg2 is required for Postgres to work with SQLAlchemy)
        if self._sqlalchemy_engine is None:
            self._sqlalchemy_engine = create_engine(self._db_url, pool_recycle=3600)

        self._sqlalchemy_connection = self._sqlalchemy_engine.connect()
        return self._sqlalchemy_connection

    def store_df_as_parquet(self, df, path, overwrite=False):
        """
        Stores the DataFrame as parquet
        :param df: Pandas DataFrame
        :param path: Local machine path
        :return: None
        """
        print_info(f"{df.shape[0]} records will be written to {path}")

        if os.path.exists(path):
            print_error(f"File path {path} exists!\n")
            if overwrite:
            	os.remove(path)
            return
        os.makedirs("/".join(path.split("/")[:-1]), exist_ok=True)
        df["id"] = np.arange(0, len(df), dtype=int)
        df.to_parquet(path, engine="fastparquet", index=False)


    def to_posgresql_table(self, df, table_name, schema="public", if_exists="fail"):
        """
        Stores the DataFrame as Postgresql table
        :param df: Pandas Dataframe
        :param table_name: Name of the table
        :param if_exists: {'fail', 'replace', 'append'}, default 'fail'
            How to behave if the table already exists.

            * fail: Raise a ValueError.
            * replace: Drop the table before inserting new values.
            * append: Insert new values to the existing table.
        :return:
        """
        conn = self.get_sqlalchemy_connection()
        try:
            df.to_sql(name=table_name,
                      con=conn,
                      if_exists=if_exists,
                      index=False,
                      schema=schema)
        except ValueError as e:
            print_error(e)

    def get_tables_list(self, table_schema="public"):
        """
        :param table_schema: Postgresql schema. Default is `public`
        :return: List of tables on given table schema
        """
        conn = self.get_sqlalchemy_connection()

        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema='public' AND table_type='BASE TABLE'
        """
        return pd.read_sql(query, conn)["table_name"].values

    def get_table(self, table_name):
        """
        Use to get the Postgresql table as Pandas dataframe
        :param table_name:
        :return: Pandas DataFrame
        """
        conn = self.get_sqlalchemy_connection()
        return pd.read_sql(f"select * from {table_name}", conn)

    def run_query(self, query):
        print_info(f"Runing query : {query}")
        sql = text(query)
        result = self._sqlalchemy_engine.execute(sql)
        return result

    def query_to_df(self, query):
        print_info(f"Runing query : {query}")
        conn = self.get_sqlalchemy_connection()
        return pd.read_sql_query(query, conn)



flags.DEFINE_string("mode", "upload", "[download/upload] tables")
flags.DEFINE_string("version", "0", "tabels version")

FLAGS = flags.FLAGS


# Tweak it for yor needs!
def main(argv):
    db = PostgresqlConnection()

    def dump(table_name):
        df = db.get_table(table_name)
        df.to_parquet("data/download/" + table_name + ".parquet", engine="fastparquet")

    def upload(file_name):
        df = pd.read_parquet("data/upload/" + file_name + ".parquet", engine="fastparquet")
        print_info(df.dtypes)
        db.to_posgresql_table(df=df, table_name=file_name, if_exists="fail")

    if FLAGS.mode == "download":
        dump("train_"+FLAGS.version)
        dump("test_"+FLAGS.version)
        dump("dev_"+FLAGS.version)
    else:
        upload("train_" + FLAGS.version)
        upload("test_" + FLAGS.version)
        upload("dev_" + FLAGS.version)


if __name__ == "__main__":
    app.run(main)
