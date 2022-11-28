from utils.postgres_db_connection import PostgresDBConnection
from utils.mysql_db_connection import MySQLDBConnection
import pandas as pd
from logger.logger import MongoLogger


class DataIngestion:
    """
    Ingests data from database using the user-specified query and database credentials.
    """

    def __init__(self, name: str, query: str, host: str, database: str, username: str, password: str, port: str):
        self.name = name
        self.query = query
        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self.port = port

    def ingest_data(self):
        logger = MongoLogger()
        logger.log_to_db(level="INFO", message="entering data_ingestion")
        df = None

        try:
            if self.name == "postgres":
                db = PostgresDBConnection(host=self.host, database=self.database,
                                          username=self.username, password=self.password, port=self.port)
                db_conn = db.connect()  # returns a postgres db connection object
            else:
                db = MySQLDBConnection(host=self.host, username=self.username, password=self.password, port=self.port)
                db_conn = db.connect()  # returns a mysql db connection object

            df = pd.read_sql(self.query, db_conn)  # reading data from database to data frame
            db_conn.close()

        except Exception as e:
            logger.log_to_db(level="CRITICAL", message=f"unexpected data_ingestion error: {e}")
            db_conn.close()
            raise

        logger.log_to_db(level="INFO", message="exiting data_ingestion")
        return df
