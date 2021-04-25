import psycopg2


class DBConnectorException(ValueError):
    pass


class DBConnector:
    def __init__(self, dbname='', user='', password='', host=''):
        if dbname == '' or user == '' or password == '' or host == '':
            raise DBConnectorException

        self.connector = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)

    def getTestData(self):
        cur = self.connector.cursor()
        cur.execute("SELECT Content FROM DataSet")
