import psycopg2
import sqlite3
import mysql.connector
import json


class DBConnectorException(ValueError):
    pass


class DBConnector:
    def __init__(self):
        self.connector = []
        with open('config.json', 'r') as file:
            config = json.load(file)

        for dbms in config:

            if config[dbms]['db'] == '':
                raise DBConnectorException

            if config[dbms]['DBMS'] == 'PostgreSQL':
                if config[dbms]["user"] == '' or config[dbms]["password"] == '' or \
                        config[dbms]["ip"] == '' or config[dbms]["port"] == '':
                    raise DBConnectorException
                self.connector.append(psycopg2.connect(dbname=config[dbms]['db'],
                                                       user=config[dbms]["user"],
                                                       password=config[dbms]["password"],
                                                       host=config[dbms]["ip"],
                                                       port=config[dbms]["port"]))
            elif config[dbms]['DBMS'] == 'MySQL':
                if config[dbms]["user"] == '' or config[dbms]["password"] == '' or \
                        config[dbms]["ip"] == '' or config[dbms]["port"] == '':
                    raise DBConnectorException
                self.connector.append(mysql.connector.connect(database=config[dbms]['db'],
                                                              user=config[dbms]["user"],
                                                              password=config[dbms]["password"],
                                                              host=config[dbms]["ip"],
                                                              port=config[dbms]["port"]))
            elif config[dbms]['DBMS'] == 'SQLite':
                self.connector.append(sqlite3.connect(config[dbms]['db']))
            else:
                raise DBConnectorException

    def getExperimentsData(self, experimentNumber):
        cur = self.connector[0].cursor()
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

    def getTablesData(self, tables):
        if len(self.connector) < 2:
            raise DBConnectorException

        tablesData = []

        for i in range(len(self.connector)):
            cur = self.connector[i].cursor()
            cur.execute(f"SELECT * FROM {tables[i]} ORDER BY id")
            tablesData.append(cur.fetchall())
            cur.close()

        return tablesData
