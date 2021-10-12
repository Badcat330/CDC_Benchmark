from typing import NoReturn
import psycopg2
import sqlite3
import mysql.connector
import pymssql


class DBConnectorException(ValueError):
    pass


class DBConnector:
    def __init__(self, config: dict) -> None:
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

    def getTableData(self, table_name: str, pk: str) -> dict:
        tables_data = {}
        itersize = 100000
        cur_names = self.connector.cursor("columns_names")
        cur_names.execute(f"""SELECT column_name FROM information_schema.columns
                        WHERE table_name   = '{table_name}'""")
        columns_names = ''.join(str(i[0]) + ', ' for i in cur_names.fetchall())
        clean_columns_names = columns_names.replace(f"{pk}, ", "").strip()[:-1]
        cur_names.close()

        cur_columns = self.connector.cursor("columns_columns")
        cur_columns.itersize = itersize
        cur_columns.execute(f"""SELECT {clean_columns_names} FROM {table_name} ORDER BY {pk}""")

        cur_id = self.connector.cursor("columns_id")
        cur_id.itersize = itersize
        cur_id.execute(f"""SELECT {pk} FROM {table_name} ORDER BY {pk}""")

        tables_data['values'] = cur_columns.fetchmany(itersize)
        tables_data['keys'] = [i[0] for i in cur_id.fetchmany(itersize)]

        while tables_data['values'] != [] and tables_data['keys'] != []:
            yield tables_data
            tables_data['values'] = cur_columns.fetchmany(itersize)
            tables_data['keys'] = [i[0] for i in cur_id.fetchmany(itersize)]

        cur_id.close()
        cur_columns.close()

    def saveResults(self) -> NoReturn:
        # TODO: Think how save result in db
        pass
