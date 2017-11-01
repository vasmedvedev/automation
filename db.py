import psycopg2
import psycopg2.extras


class db_connection(object):

    def __init__(self, config):
        """
        Context manager managing connection to PostgreSQL
        :param config: parsed config file
        :type config: config.Config
        """
        self.database = config.database
        self.user = config.user
        self.password = config.password
        self.db_host = config.db_host
        self.port = config.port

    def __enter__(self):
        self.conn = psycopg2.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.db_host,
            port=self.port
        )
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()
