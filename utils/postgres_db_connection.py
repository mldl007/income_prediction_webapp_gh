import psycopg2
from logger.logger import MongoLogger


class PostgresDBConnection:
    """
    returns connection object of the specified Postgres database.
    """

    def __init__(self, host: str, database: str, username: str, password: str, port: str = "5432"):
        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.db_connection = None

    def connect(self):  # method to return connection object
        logger = MongoLogger()
        try:
            db_name = self.database
            # if self.host.startswith("localhost"):
            #     db_name = "postgres"
            self.db_connection = psycopg2.connect(host=self.host,
                                                  port=self.port,
                                                  database=db_name,
                                                  user=self.username,
                                                  password=self.password)
            self.db_connection.autocommit = True
        except Exception as conn_exception:
            logger.log_to_db(level="CRITICAL",
                             message=f"DB connection error: {conn_exception}")
            raise
        else:
            logger.log_to_db(level="INFO",
                             message="DB SERVER CONNECTION SUCCESSFUL")
        return self.db_connection
