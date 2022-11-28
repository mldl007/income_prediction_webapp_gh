import mysql.connector as connection
from logger.logger import MongoLogger


class MySQLDBConnection:
    def __init__(self, host: str, username: str, password: str, port: str = "3306"):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.db_connection = None

    def connect(self):
        logger = MongoLogger()
        try:
            self.db_connection = connection.connect(host=self.host,
                                                    username=self.username,
                                                    password=self.password,
                                                    port=self.port)
        except Exception as conn_exception:
            logger.log_to_db(level="CRITICAL",
                             message=f"DB connection error: {conn_exception}")
            raise
        else:
            logger.log_to_db(level="INFO",
                             message="DB SERVER CONNECTION SUCCESSFUL")
        return self.db_connection
