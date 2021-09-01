import pandas as pd
from pandas._testing import rands_array
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import json
from openpyxl import load_workbook
import os.path
import time
from timeit import default_timer as timer
from datetime import datetime
import logging


class CDCGenerator:
    def __init__(self, config_file):
        self.set_config(config_file)
        self.tb_names = pd.DataFrame(columns=["experiment_id", "tb_source", "elements", "tb_target", "changes"])
        self.start = 0
        logging.basicConfig(filename='CDCGenerator.log', level=logging.INFO)
        self.dfa = pd.DataFrame()

    def set_config(self, config_file: str):
        with open(config_file) as json_file:
            self.config = json.load(json_file)
        if self.config["rows_min_count"] == 0 or self.config["rows_max_count"] == 0 or self.config["columns_min_count"] == 0 \
                or self.config["columns_max_count"] == 0:
            print("rows_min_count,rows_max_count,columns_min_count,columns_max_count should be greater than 0 ")
            raise ValueError
        if self.config["rows_step"] == 0:
                self.rows_count=[self.config["rows_min_count"]]
        else:
                self.rows_count = [cc for cc in range(self.config["rows_min_count"],
                                                          self.config["rows_max_count"] + self.config["rows_step"],
                                                          self.config["rows_step"])]
        if self.config["columns_step"] == 0:
                self.columns_count=[self.config["columns_min_count"]]
        else:
                self.columns_count = [cc for cc in range(self.config["columns_min_count"],
                                                         self.config["columns_max_count"] + self.config["columns_step"],
                                                         self.config["columns_step"])]

    def exp_create(self):
        logging.info("!")
        logging.info(time.strftime("%H:%M:%S", time.gmtime(time.time())) + " Generating is started")
        self.start = time.time()
        dt_changes={}
        if self.config["columnsIsAbsolute"] == "True":
            logging.debug("ColumnsIsAbsolute is True. Values columns_int, columns_float,columns_string are absolut. "
                          "Values of total,columns_min_count,columns_max_count,columns_step are not using.")
            for rc in self.rows_count:
                    self.df_schema = self.get_df_schema(is_absolute=self.config["columnsIsAbsolute"], icount=self.config["columns_int"], fcount=self.config["columns_float"], scount=self.config["columns_string"])
                    self.dfa = self.get_df(rows_count=rc)
                    dfa_name = "a" + str(self.config["exp_id"]) + "_c" + str(len(self.dfa.columns)) + "_r" + str(rc)
                    self.where_to_save(self.dfa,dfa_name)
                    # make changes in the self.dfa dataframe
                    changes = self.make_rows_changes(self.dfa, ucount=self.config["updates"], dcount=self.config["deletes"], icount=self.config["inserts"])
                    dfb_name = "b" + str(self.config["exp_id"]) + "_c" + str(len(self.dfa.columns)) + "_r" + str(rc)
                    # save modified self.dfa
                    self.where_to_save(self.dfa,dfb_name)
                    dt_changes[dfa_name] = [self.config["exp_id"], dfa_name, rc, dfb_name, changes]

        elif self.config["columnsIsAbsolute"] == "False":
            logging.debug("ColumnsIsAbsolute is False. The number of the total generating automatically from columns_min_count to columns_max_count."
                         "The value of the columns_int, columns_float,columns_string variables denotes the percentage of the number of columns."
                          "Sum of columns_int, columns_float,columns_string must be equal 100%.")

            for rc in self.rows_count:
                for cc in self.columns_count:
                    self.df_schema = self.get_df_schema(is_absolute=self.config["columnsIsAbsolute"], icount=self.config["columns_int"], fcount=self.config["columns_float"], scount=self.config["columns_string"],
                                                        total=cc)
                    self.dfa = self.get_df(rows_count=rc)
                    dfa_name = "a" + str(self.config["exp_id"]) + "_c" + str(cc) + "_r" + str(rc)
                    self.where_to_save(self.dfa, dfa_name)

                    changes = self.make_rows_changes(self.dfa, ucount=self.config["updates"], dcount=self.config["deletes"], icount=self.config["inserts"])
                    dfb_name = "b" + str(self.config["exp_id"]) + "_c" + str(cc) + "_r" + str(rc)
                    self.where_to_save(self.dfa, dfb_name)
                    dt_changes[dfa_name] = [self.config["exp_id"], dfa_name, rc, dfb_name, changes]
        if self.config["save_tb_name"] == "True":
            self.tb_name_update(dt_changes)
        logging.info(str(datetime.fromtimestamp(time.time())-datetime.fromtimestamp(self.start)) + " Generating is finished")

    def exp_delete(self):
        query = ""

        if self.config["columnsIsAbsolute"] == "True":
            for rc in self.rows_count:
                self.df_schema = self.get_df_schema(is_absolute=self.config["columnsIsAbsolute"], icount=self.config["columns_int"], fcount=self.config["columns_float"], scount=self.config["columns_string"])
                self.dfa = self.get_df(rows_count=1)
                dfa_name = "a" + str(self.config["exp_id"]) + "_c" + str(len(self.dfa.columns)) + "_r" + str(rc)
                dfb_name = "b" + str(self.config["exp_id"]) + "_c" + str(len(self.dfa.columns)) + "_r" + str(rc)

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
                print('Total percentage should be 100%')
                raise ValueError
            icount = int(total * (icount / 100))
            fcount = int(total * (fcount / 100))
            scount = int(total * (scount / 100))
            if (icount+fcount+scount)<total:
                icount += total-(icount+fcount+scount)
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

    def get_df(self, rows_count=1) -> pd.DataFrame:
        """ df_schema - dict in form {"<column name>":<column value>}
            rows_count - number of rows in the Dataframe
            char_count - number of chars in the strings
        """
        char_count = self.config["rnd_char_count"]
        df = pd.DataFrame(self.df_schema, index=[])
        for cname, ctype in self.df_schema.items():
            if type(ctype) == int:
                df[cname] = pd.Series(np.random.randint(0, 100000, size=rows_count))
            elif type(ctype) == float:
                df[cname] = pd.Series(np.random.rand(rows_count))
            elif type(ctype) == str:
                df[cname] = pd.Series(rands_array(char_count, rows_count))
        return df

    def make_rows_changes(self, dataframe, ucount=1, dcount=0, icount=0):
        """
            dataframe - pandas DataFrame, that will be changed
            isAbsolut - Values of ucount,dcount,icount mean absolute numbers or percents from all rows at dataset.
            ucount - the number of rows to update
            dcount - the number of rows to update
            icount - the number of rows to update
        """
        #dataframe = dataframe.copy()
        len_df = len(dataframe)
        clms = list(dataframe.columns)

        if self.config["changesIsAbsolute"] is False:
            if icount + dcount + ucount != 100:
                print('Total percentage should be 100%')
                raise ValueError
            icount = int(len_df * (icount / 100))
            ucount = int(len_df * (dcount / 100))
            dcount = int(len_df * (ucount / 100))

        # make updates
        update_indexes = np.random.randint(0, len_df, size=ucount)  # get indexes of rows to make inserts
        changes = []
        # format for storing changes
        # [{"op":"u","id":0,"v":1}, {"op":"d","id":1}, {"op":"i","id":2,"v":"json"}]
        for i in update_indexes:
            val = json.loads(dataframe[clms].iloc[i].to_json())
            changes.append({"op":"u","id":int(i),"v":val})
            dataframe.iloc[i] = self.get_df(rows_count=1).iloc[0]
        # make inserts
        first_index = int(dataframe.tail(1).index.item())+1
        for i in range(first_index, first_index + icount):
            rnd_val = self.get_df(rows_count=1).iloc[0]  # get new random element with 1 element
            #dataframe[i] = rnd_val
            dataframe = dataframe.append(rnd_val,ignore_index=True)
            changes.append({"op": "i", "id": int(i), "v": json.loads(rnd_val.to_json())})
        if dcount> len(dataframe):
            logging.info(" dcount must more than len(dataframe)")
            raise  ValueError
        # make deletes
        delete_indexes = list(np.random.randint(0, len(dataframe), size=dcount))
        dataframe.drop(delete_indexes, inplace=True)
        for i in delete_indexes:
            changes.append({"op": "d","id": int(i)})
            # нужно ли сохранять что было удалено?
        changes = json.dumps(changes)
        return  changes

    def tb_name_update(self,dt_changes=None,op="append"):
        if op=="append" and dt_changes is not None:
            clms=["experiment_id", "tb_source", "elements", "tb_target", "changes"]
            df_tb = pd.DataFrame.from_dict(dt_changes,orient="index",columns=clms)
            connection_string = self.config["db_cs"]
            conn = create_engine(connection_string)
            df_tb.to_sql("tb_names", conn, if_exists='append',index=False)
        elif op == "delete":
            query = "DELETE FROM tb_names WHERE experiment_id = " + str(self.config["exp_id"]) + ";"
            connection_string = self.config["db_cs"]
            engine = create_engine(connection_string)
            with engine.connect() as con:
                statement = text(query)
                con.execute(statement)

    def where_to_save(self,dataframe,df_name):
        if self.config["where_to_save"] == "db":
            temp_time = time.time()
            self.save_to_db(dataframe, df_name)
            logging.info(time.strftime("%H:%M:%S", time.gmtime(time.time()-temp_time)) + " takes to save table "+df_name+" to the db")
        elif self.config["where_to_save"] == "excel":
            temp_time = time.time()
            self.save_to_excel(dataframe,str(self.start)+ ".xlsx",df_name)
            logging.info(time.strftime("%H:%M:%S", time.gmtime(time.time()-temp_time)) + " takes to save table "+df_name+" to the excel file")

    def save_to_db(self, dataframe: pd.DataFrame, tb_name):
        try:
            #"db_cs": "postgresql://u_cdc:P@ssword2021@92.242.58.173:1984/cdc1"
            #"db_cs": "postgresql://u_cdc:P@ssword2021@192.168.2.23:1984/cdc1"
            connection_string = self.config["db_cs"]
            conn = create_engine(connection_string)
            dataframe.to_sql(tb_name, conn, if_exists='replace',index=False)
        except Exception as e:
            print(e)

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
            print(e)

if __name__ == '__main__':
    gen = CDCGenerator('CDCGenerator_config.json')
    gen.exp_create()
    #gen.exp_delete()