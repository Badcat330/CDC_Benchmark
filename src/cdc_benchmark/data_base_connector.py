import psycopg2
import sqlite3
import mysql.connector
import json
import pymssql
import uuid
import datetime


class DBConnectorException(ValueError):
    pass


class DBConnector:
    def __init__(self):
        self.connector = {}
        with open('config.json', 'r') as file:
            config = json.load(file)

        self.connector['source'] = self.addDBMS(config['source'])
        self.connector['destination'] = self.addDBMS(config['destination'])
        self.connector['benchDB'] = self.addDBMS(config['benchDB'])
        self.config = config

    def addDBMS(self, DBMS):
        if DBMS['db'] == '':
            raise DBConnectorException

        if DBMS['DBMS'] == 'PostgreSQL':
            if DBMS["user"] == '' or DBMS["password"] == '' or \
                    DBMS["ip"] == '' or DBMS["port"] == '':
                raise DBConnectorException
            return psycopg2.connect(dbname=DBMS['db'],
                                    user=DBMS["user"],
                                    password=DBMS["password"],
                                    host=DBMS["ip"],
                                    port=DBMS["port"])
        elif DBMS['DBMS'] == 'MySQL':
            if DBMS["user"] == '' or DBMS["password"] == '' or \
                    DBMS["ip"] == '' or DBMS["port"] == '':
                raise DBConnectorException
            return mysql.connector.connect(database=DBMS['db'],
                                           user=DBMS["user"],
                                           password=DBMS["password"],
                                           host=DBMS["ip"],
                                           port=DBMS["port"])
        elif DBMS['DBMS'] == 'SQLite':
            return sqlite3.connect(DBMS['db'])
        elif DBMS['DBMS'] == 'SQLServer':
            if DBMS["user"] == '' or DBMS["password"] == '' or \
                    DBMS["ip"] == '' or DBMS["port"] == '':
                raise DBConnectorException
            return pymssql.connect(server=DBMS["ip"],
                                   user=DBMS["user"],
                                   password=DBMS["password"],
                                   database=DBMS['db'])
        else:
            raise DBConnectorException

    def getExperimentsData(self, experimentNumber):
        cur = self.connector['source'].cursor()
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

        for i in [('source', 0), ('destination', 1)]:
            cur = self.connector[i[0]].cursor()
            cur.execute(f"SELECT * FROM {tables[i[1]]} ORDER BY id")
            tablesData.append(cur.fetchall())
            cur.close()

        return tablesData

    def saveResults(self, results, tables):
        program_id = 1
        ts = datetime.datetime.now().timestamp()
        db_info = dict(self.config)
        del db_info['benchDB']
        db_info['source']['table'] = tables[0]
        db_info['destination']['table'] = tables[1]

        cur = self.connector['benchDB'].cursor()
        cur.execute(f"INSERT INTO tb_exp_run (program_id, timestamp, efficiency, db_info, changes)"
                    f"VALUES ({program_id}, {ts}, {results['Efficiency']}, {db_info}, {results}")
        cur.close()