import psycopg2
import sqlite3
import mysql.connector
import pymssql



class DBConnectorException(ValueError):
    pass


class DBConnector:
    def __init__(self, config):
        if config['db'] == '':
            raise DBConnectorException

        if config['DBMS'] == 'PostgreSQL':
            if config["user"] == '' or config["password"] == '' or \
                    config["ip"] == '' or config["port"] == '':
                raise DBConnectorException
            self.connector = psycopg2.connect(dbname=config['db'],
                                    user=config["user"],
                                    password=config["password"],
                                    host=config["ip"],
                                    port=config["port"])
        elif config['DBMS'] == 'MySQL':
            if config["user"] == '' or config["password"] == '' or \
                    config["ip"] == '' or config["port"] == '':
                raise DBConnectorException
            self.connector = mysql.connector.connect(database=config['db'],
                                           user=config["user"],
                                           password=config["password"],
                                           host=config["ip"],
                                           port=config["port"])
        elif config['DBMS'] == 'SQLite':
            self.connector = sqlite3.connect(config['db'])
        elif config['DBMS'] == 'SQLServer':
            if config["user"] == '' or config["password"] == '' or \
                    config["ip"] == '' or config["port"] == '':
                raise DBConnectorException
            self.connector = pymssql.connect(server=config["ip"],
                                   user=config["user"],
                                   password=config["password"],
                                   database=config['db'])
        else:
            raise DBConnectorException
        

    def getTableData(self, table_name, pk):
        cur = self.connector.cursor()
        tablesData = {}

        cur.execute(f"""
        SELECT * INTO TemporaryTable FROM {table_name};
        ALTER TABLE TemporaryTable DROP COLUMN {pk};
        SELECT * FROM TemporaryTable;
        """)

        tablesData['values'] = cur.fetchall()

        cur.execute(f"""
        DROP TABLE TemporaryTable;
        SELECT {pk} FROM {table_name};
        """)

        tablesData['keys'] = cur.fetchall()

        cur.close()

        return tablesData

    def saveResults(self, results, tables):
        #TODO: Think how save result in db
        pass