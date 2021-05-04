import psycopg2
import sqlite3


class DBConnectorException(ValueError):
    pass


class DBConnector:
    def __init__(self, dbtype='', dbname='', user='', password='', host='', port=''):
        if dbname == '':
            raise DBConnectorException

        if dbtype == 'PostgreSQL':
            if user == '' or password == '' or host == '' or port == '':
                raise DBConnectorException
            self.connector = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        elif dbtype == 'SQLite':
            self.connector = sqlite3.connect(dbname)
        else:
            raise DBConnectorException

    def getExperimentsData(self, experimentNumber=1):
        cur = self.connector.cursor()
        cur.execute(f"SELECT tb_name FROM experiments1 WHERE experiment_id = {experimentNumber} ORDER BY elements")
        tables = cur.fetchall()
        for table in tables:
            cur.execute(f"SELECT dp1 FROM {table[0]}")
            yield list(map(lambda x: x[0], cur.fetchall()))
        cur.close()
