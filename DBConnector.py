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

    def getExperimentsData(self, experimentNumber):
        cur = self.connector.cursor()
        cur.execute(f"SELECT tb_source, tb_target, changes FROM tb_names WHERE experiment_id = {experimentNumber}"
                    f" ORDER BY elements")
        tables = cur.fetchall()
        for table in tables:
            cur.execute(f"SELECT id, value FROM {table[0]} ORDER BY id")
            tb_source = cur.fetchall()
            cur.execute(f"SELECT id, value FROM {table[1]} ORDER BY id")
            tb_target = cur.fetchall()
            changes = table[2]
            yield tb_source, tb_target, changes
        cur.close()
