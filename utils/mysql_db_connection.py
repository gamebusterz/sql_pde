import pymysql
import traceback


class MySQLDBConnection(object):
    def __init__(self, host, user, password, db, cursorclass=pymysql.cursors.Cursor, logger=None):
        self.connection = None
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.cursorclass = cursorclass
        self.logger = logger

    def __enter__(self):
        self.connection = self.create_conn(self.host, self.user, self.password, self.db, self.cursorclass)
        if self.logger:
            self.logger.info('DW CONNECTION OPENED!')
        return self.connection

    def __exit__(self, type, value, traceback):
        self.connection.commit()
        self.connection.close()
        if self.logger:
            self.logger.info('DW CONNECTION CLOSED.')

    def create_conn(self, host, user, password, db, cursorclass):
        connection = pymysql.connect(host=host, user=user, password=password, db=db, cursorclass=cursorclass)
        return connection
