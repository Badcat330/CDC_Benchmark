import pandas as pd
from pandas._testing import rands_array
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import json
from openpyxl import load_workbook
import os.path
import time
from datetime import datetime
import logging
import psutil
import math
import psycopg2


class CDCGenerator:
    def __init__(self, config_file = "temporary/generator_config.json"):
        self.set_config(config_file)
        self.tb_names = pd.DataFrame(columns=["experiment_id", "tb_source", "elements", "tb_target", "changes"])
        self.start = 0
        self.dfa = pd.DataFrame()
        self.df_schema = ""
        self.current_tb_rows = ""

    def set_config(self, config_file: str):
        with open(config_file) as json_file:
            self.config = json.load(json_file)
        if self.config["rows_min_count"] == 0 or self.config["rows_max_count"] == 0 or self.config[
            "columns_min_count"] == 0 \
                or self.config["columns_max_count"] == 0:
            logging.info("rows_min_count,rows_max_count,columns_min_count,columns_max_count should be greater than 0 ")
            raise ValueError
        if self.config["rows_step"] == 0:
            self.rows_count = [self.config["rows_min_count"]]
        else:
            self.rows_count = [cc for cc in range(self.config["rows_min_count"],
                                                  self.config["rows_max_count"] + self.config["rows_step"],
                                                  self.config["rows_step"])]
        if self.config["columns_step"] == 0:
            self.columns_count = [self.config["columns_min_count"]]
        else:
            self.columns_count = [cc for cc in range(self.config["columns_min_count"],
                                                     self.config["columns_max_count"] + self.config["columns_step"],
                                                     self.config["columns_step"])]
        if self.config["logging level"] == "debug":
            logging.basicConfig(filename='temporary/CDCGenerator.log', level=logging.DEBUG)
        elif self.config["logging level"] == "info":
            logging.basicConfig(filename='temporary/CDCGenerator.log', level=logging.INFO)

    def exp_delete(self):
        query = ""
        if self.config["columnsIsAbsolute"] == "True":
            for rc in self.rows_count:
                self.df_schema = self.get_df_schema(is_absolute=self.config["columnsIsAbsolute"],
                                                    icount=self.config["columns_int"],
                                                    fcount=self.config["columns_float"],
                                                    scount=self.config["columns_string"])
                self.dfa = self.get_rnd_df(rows_count=1)
                dfa_name = "a" + str(self.config["exp_id"]) + "_c" + str(len(self.dfa.columns)) + "_r" + str(rc)
                dfb_name = self.config["btable_prefix"] + str(self.config["exp_id"]) + "_c" + str(
                    len(self.dfa.columns)) + "_r" + str(rc)
                if query == "":
                    query += dfa_name
                    query += "," + dfb_name
                else:
                    query += "," + dfa_name
                    query += "," + dfb_name
            if (query != "") | (query == "*"):
                query = "DROP TABLE " + query + ";"
            connection_string = self.config["db_cs"]
            engine = create_engine(connection_string)
            with engine.connect() as con:
                statement = text(query)
                con.execute(statement)
            self.tb_name_update(op="delete")

        elif self.config["columnsIsAbsolute"] == "False":
            for rc in self.rows_count:
                for cc in self.columns_count:
                    dfa_name = "a" + str(self.config["exp_id"]) + "_c" + str(cc) + "_r" + str(rc)
                    dfb_name = "b" + str(self.config["exp_id"]) + "_c" + str(cc) + "_r" + str(rc)

                    if query == "":
                        query += dfa_name
                        query += "," + dfb_name
                    else:
                        query += "," + dfa_name
                        query += "," + dfb_name
            if (query != "") | (query == "*"):
                query = "DROP TABLE " + query + ";"
            connection_string = self.config["db_cs"]
            engine = create_engine(connection_string)
            with engine.connect() as con:
                statement = text(query)
                con.execute(statement)
            self.tb_name_update(op="delete")

    def get_df_schema(self, is_absolute=True, icount=1, fcount=0, scount=0, total=0) -> dict:
        """ isAbsolute - icount,fcount,scount contain absolute numbers or percents from total
            icount - number of integer columns
            fcount - number of float columns
            scount - number of string columns
            total - total number of elements in the result dataframe. If isAbsolute is True, total is ignoring
        """
        df_schema = {}
        if is_absolute == "False":
            if (icount + fcount + scount) != 100:
                logging.info('Total percentage should be 100%')
                raise ValueError
            icount = int(total * (icount / 100))
            fcount = int(total * (fcount / 100))
            scount = int(total * (scount / 100))
            if (icount + fcount + scount) < total:
                icount += total - (icount + fcount + scount)
        for i in range(icount):
            col_name = "i" + str(i)
            df_schema[col_name] = int()
        for i in range(fcount):
            col_name = "f" + str(i)
            df_schema[col_name] = float()
        for i in range(scount):
            col_name = "s" + str(i)
            df_schema[col_name] = str()
        return df_schema

    def get_rnd_df(self, rows_count=1, indexes=None) -> pd.DataFrame:
        """ df_schema - dict in form {"<column name>":<column value>}
            rows_count - number of rows in the Dataframe
            char_count - number of chars in the strings
        """
        if indexes is None:
            df = pd.DataFrame(self.df_schema, index=[])
        elif indexes is not None:
            df = pd.DataFrame(self.df_schema, index=indexes)
        for cname, ctype in self.df_schema.items():
            if type(ctype) == int:
                df[cname] = pd.Series(np.random.randint(0, 100000, size=rows_count))
            elif type(ctype) == float:
                df[cname] = pd.Series(np.random.rand(rows_count))
            elif type(ctype) == str:
                df[cname] = pd.Series(rands_array(self.config["rnd_char_count"], rows_count))
        return df

    def get_rnd_val(self, df, column_name):
        ret_val = None
        if column_name in list(df.columns):
            ctype = str(df.dtypes[column_name])
            if "int" in ctype:
                ret_val = np.random.randint(0, 100000)
            elif "float" in ctype:
                ret_val = np.random.rand(1)[0]
            elif "object" in ctype:
                ret_val = rands_array(self.config["rnd_char_count"], 1)[0]
        return ret_val

    def copy_table(self, atable: str, btable: str):
        try:
            conn = psycopg2.connect(self.config["db_cs"])
            query = "SELECT * INTO " + btable + " FROM " + atable + ";"
            query2 = "ALTER TABLE " + btable + " ADD CONSTRAINT pk_" + btable + " PRIMARY KEY (id);"
            with conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    logging.debug("sql: " + query)
                    cur.execute(query2)
                    logging.debug("sql: " + query2)
        except  Exception as e:
            logging.info("copy_table: " + atable + " " + str(e))
        finally:
            if conn:
                conn.close()

    def get_rows_num(self, tb_name):
        rows_num = 0
        if "e" in tb_name:
            zeros = "0" * int(tb_name.split("e")[1])
            rows_num = int("1" + zeros)
        elif "r" in tb_name:
            rows_num = int(tb_name.split("r")[1])
        return rows_num

    def set_tb_schema(self, tb_name):
        sql = "SELECT * FROM " + tb_name + " LIMIT 1"
        df_t = pd.read_sql_query(sql, create_engine(self.config["db_cs"]))
        self.tb_columns = [name for name in df_t.columns]
        if "id" in self.tb_columns:
            self.tb_columns.remove("id")
        self.df_schema = {}
        for col_name in self.tb_columns:
            if "i" in col_name:
                self.df_schema[col_name] = int()
            elif "f" in col_name:
                self.df_schema[col_name] = float()
            elif "s" in col_name:
                self.df_schema[col_name] = str()
        self.current_tb_rows = self.get_rows_num(tb_name)

    def get_rnd_arr(self, tb_name: str, ucount: int) -> list:
        sql = "SELECT * FROM " + tb_name + " LIMIT 1"
        df_t = pd.read_sql_query(sql, create_engine(self.config["db_cs"]))
        self.tb_columns = [name for name in df_t.columns]
        indexes = np.random.randint(0, self.get_rows_num(tb_name), size=ucount)
        ret_arr = []  # [{"op":"u","id":0,"v":1,"colid":""},...]
        for ui in indexes:
            dd = {}
            cname_id = np.random.randint(0, len(self.tb_columns))
            dd["op"] = "u"
            dd["id"] = ui  # this is row id
            dd["colid"] = self.tb_columns[cname_id]
            dd["v"] = self.get_rnd_val(df_t, self.tb_columns[cname_id])
            ret_arr.append(dd)
        return ret_arr

    def update_table(self, tb_name: str, values: list):
        try:
            conn = psycopg2.connect(self.config["db_cs"])
            cur = conn.cursor()
            for i in values:
                if type(i["v"]) == str:
                    query = "UPDATE  {table_name} SET {column_name} = \'{value}\'  WHERE id={id}".format(
                        table_name=tb_name, column_name=i["colid"], value=i["v"], id=i["id"])
                else:
                    query = "UPDATE  {table_name} SET {column_name} = {value}  WHERE id={id}".format(table_name=tb_name,
                                                                                                     column_name=i[
                                                                                                         "colid"],
                                                                                                     value=i["v"],
                                                                                                     id=i["id"])
                logging.debug("sql: " + query)
                cur.execute(query)
                conn.commit()
        except (Exception, psycopg2.Error) as error: \
                logging.info("Error: update_table: " + str(error))
        finally:
            if conn:
                cur.close()
                conn.close()

    def sql_execute(self, query: str):
        try:
            conn = psycopg2.connect(self.config["db_cs"])
            cur = conn.cursor()
            logging.debug("sql_execute:sql: " + query)
            cur.execute(query)
            conn.commit()
        except (Exception, psycopg2.Error) as error: \
                logging.info("Error: update_table: " + str(error))
        finally:
            if conn:
                cur.close()
                conn.close()

    def delete_rows(self, tb_name: str, ids: list):
        try:
            if len(ids) > 0:
                conn = psycopg2.connect(self.config["db_cs"])
                cur = conn.cursor()
                for i in ids:
                    query = "DELETE FROM {table_name} WHERE id={id}".format(table_name=tb_name, id=i)
                    logging.debug("sql: " + query)
                    cur.execute(query)
                conn.commit()
        except (Exception, psycopg2.Error) as error: \
                logging.info("Error: update_table: " + str(error))
        finally:
            if conn:
                cur.close()
                conn.close()

    def make_rows_changes(self, atable, ucount=1, icount=0, dcount=0):
        # 1. Copy a-table to the b-table
        # 2. Make updates
        # 3. Make inserts
        # 4. Make deletes
        # Get name of the target table
        logging.info("!")
        logging.info(time.strftime("%H:%M:%S", time.gmtime(time.time())) + " Changing is started. ")
        self.chstart = time.time()

        btable = self.config["btable_prefix"] + atable[1:]  # b-table name
        if (self.df_schema == "") | (self.current_tb_rows == ""):
            self.set_tb_schema(atable)
        # 1. Copy a-table to the b-table
        if self.config["copy_btable"] == "True":
            self.copy_table(atable, btable)
        uvals = []
        # 2. Make updates
        # 2.1. Generate new random indexes
        if ucount > 0:
            uvals = self.get_rnd_arr(atable, ucount)
            # 2.2. Save changes to the b-table
            self.update_table(btable, uvals)
        logging.info("Updates finished")
        # 3. Make inserts
        if icount > 0:
            idf = self.get_rnd_df(rows_count=icount)
            idf["id"] = list(range(self.current_tb_rows + 1, self.current_tb_rows + icount + 1))
            logging.debug("INSERT ids:" + str(idf["id"].to_list()))
            self.where_to_save(idf, btable, "append")
            for index, row in idf.iterrows():
                uvals.append({"op": "i", "id": row["id"], "v": row.to_json()})
        logging.info("Inserts finished")
        # 4. Make deletes
        if dcount > 0 & dcount < (self.current_tb_rows + icount + 1):
            delete_ids = list(np.random.randint(0, (self.current_tb_rows + icount), size=dcount))
            self.delete_rows(btable, delete_ids)
            for i in delete_ids:
                uvals.append({"op": "d", "id": int(i)})
        changes = pd.DataFrame(uvals).to_json(orient="records")
        logging.debug("changes: " + changes)
        if self.config["save_tb_name"] == "True":
            self.tb_name_update({atable: [self.config["exp_id"], atable, self.current_tb_rows, btable, changes]})
        logging.info(
            str(datetime.fromtimestamp(time.time()) - datetime.fromtimestamp(self.chstart)) + " Changing is finished")

    def tb_name_update(self, dt_changes=None, op="append"):
        if op == "append" and dt_changes is not None:
            clms = ["experiment_id", "tb_source", "elements", "tb_target", "changes"]
            df_tb = pd.DataFrame.from_dict(dt_changes, orient="index", columns=clms)
            conn = create_engine(self.config["db_cs"])
            df_tb.to_sql("tb_names", conn, if_exists='append', index=False)
        elif op == "delete":
            query = "DELETE FROM tb_names WHERE experiment_id = " + str(self.config["exp_id"]) + ";"
            engine = create_engine(self.config["db_cs"])
            with engine.connect() as con:
                statement = text(query)
                con.execute(statement)

    def where_to_save(self, dataframe: pd.DataFrame, df_name: str, if_exists="append", save_index=False):
        if self.config["where_to_save"] == "db":
            temp_time = time.time()
            try:
                # "db_cs": "postgresql://u_cdc:....@92.242.58.173:1984/cdc1"
                # "db_cs": "postgresql://u_cdc:....@192.168.2.23:1984/cdc1"
                # conn = create_engine(connection_string,fast_executemany=True)
                # "postgresql://u_cdc:PAasword2021@20.79.249.191:5432/cdc1",
                conn = create_engine(self.config["db_cs"])
                dataframe.to_sql(df_name, conn, if_exists=if_exists, index=save_index)
                if save_index is True:
                    q = "ALTER TABLE " + df_name + " RENAME index TO id;"
                    self.sql_execute(q)
                    q2 = "ALTER TABLE " + df_name + " ADD CONSTRAINT " + df_name + "_pk PRIMARY KEY(id);"
                    self.sql_execute(q2)
            except Exception as e:
                logging.info("Error: save_to_db: " + str(e))
            logging.info(time.strftime("%H:%M:%S", time.gmtime(
                time.time() - temp_time)) + " takes to save table " + df_name + " to the db")
        elif self.config["where_to_save"] == "excel":
            temp_time = time.time()
            self.save_to_excel(dataframe, str(self.start) + ".xlsx", df_name)
            logging.info(time.strftime("%H:%M:%S", time.gmtime(
                time.time() - temp_time)) + " takes to save table " + df_name + " to the excel file")

    def save_to_excel(self, dataframe: pd.DataFrame, file_name, sheet_name="Sheet1"):
        try:
            if os.path.isfile(file_name):
                book = load_workbook(file_name)
                writer = pd.ExcelWriter(file_name, engine='openpyxl')
                writer.book = book
                writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
                dataframe.to_excel(writer, sheet_name)
                writer.save()
            else:
                dataframe.to_excel(file_name, sheet_name)
        except Exception as e:
            logging.info("Error: save_to_excel: " + str(e))

    def get_indexes(self, total_rows: int):
        # `посчитать объем памяти занимаемый одной строкой в пандас. количество столбцов не более 1000
        # chunk_size = общий объем памяти разделить на объем одной строки
        df_one_row = self.get_rnd_df(rows_count=1)
        row_size = df_one_row.memory_usage(index=True).sum()  # size in bytes
        df_size = int(math.ceil(psutil.virtual_memory().available / (row_size)))
        logging.info("Row size,bytes: " + str(row_size))
        df_size = 100000
        min_idx = 0
        max_idx = df_size
        indexes = []
        if df_size >= total_rows:
            indexes.append(list(range(min_idx, total_rows)))
        else:
            while max_idx <= total_rows:
                idx = list(range(min_idx, max_idx))
                indexes.append(idx)
                min_idx += df_size
                max_idx += df_size
        return indexes

    def exp_create(self):
        logging.info("!")
        logging.info(time.strftime("%H:%M:%S", time.gmtime(time.time())) + " Generating is started. Exp id: " + str(
            self.config["exp_id"]))
        self.start = time.time()
        if self.config["columnsIsAbsolute"] == "True":
            logging.debug("ColumnsIsAbsolute is True. Values columns_int, columns_float,columns_string are absolut. "
                          "Values of total,columns_min_count,columns_max_count,columns_step are not using.")
            for rc in self.rows_count:
                self.current_tb_rows = rc
                self.df_schema = self.get_df_schema(is_absolute=self.config["columnsIsAbsolute"],
                                                    icount=self.config["columns_int"],
                                                    fcount=self.config["columns_float"],
                                                    scount=self.config["columns_string"])
                indexes_list = self.get_indexes(rc)
                dfa_name = "a" + str(self.config["exp_id"]) + "_c" + str(len(self.df_schema)) + "_r" + str(rc)
                for idx in indexes_list:
                    self.dfa = self.get_rnd_df(rows_count=rc, indexes=idx)
                    self.where_to_save(self.dfa, dfa_name, save_index=True)
                self.make_rows_changes(dfa_name, ucount=self.config["updates"], dcount=self.config["deletes"],
                                       icount=self.config["inserts"])

        elif self.config["columnsIsAbsolute"] == "False":
            logging.debug(
                "ColumnsIsAbsolute is False. The number of the total generating automatically from columns_min_count to columns_max_count."
                "The value of the columns_int, columns_float,columns_string variables denotes the percentage of the number of columns."
                "Sum of columns_int, columns_float,columns_string must be equal 100%.")
            for rc in self.rows_count:
                for cc in self.columns_count:
                    pass
        logging.info(
            str(datetime.fromtimestamp(time.time()) - datetime.fromtimestamp(self.start)) + " Generating is finished")
