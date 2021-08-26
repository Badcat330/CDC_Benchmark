import pandas as pd
from pandas.util.testing import rands_array
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import json
from openpyxl import load_workbook
import os.path


class CDCGeneratorException(ValueError):
    pass


class CDCGenerator:
    def __init__(self, config_file):
        self.set_config(config_file)
        self.tb_names = pd.DataFrame(columns=["experiment_id","tb_source","elements","tb_target","changes"])

    def set_config(self, config_file: str):
        with open(config_file) as json_file:
            self.config = json.load(json_file)
        self.rows_count = [cc for cc in range(self.config["rows_min_count"],
                                                  self.config["rows_max_count"] + self.config["rows_step"],
                                                  self.config["rows_step"])]
        self.columns_count = [cc for cc in range(self.config["columns_min_count"],
                                                 self.config["columns_max_count"] + self.config["columns_step"],
                                                 self.config["columns_step"])]
        self.df_schema = self.get_df_schema(is_absolute=self.config["columnsIsAbsolute"],
                                            icount=self.config["columns_int"],
                                            fcount=self.config["columns_float"],
                                            scount=self.config["columns_string"])

    def exp_create(self):
        dt_changes={}
        for rc in self.rows_count:
            for cc in self.columns_count:
                dfa = self.get_df(rows_count=rc)
                dfb,changes = self.make_df_changes(dfa, is_absolute=self.config["changesIsAbsolute"],
                                           ucount=self.config["updates"],
                                           dcount=self.config["deletes"],
                                           icount=self.config["inserts"])
                dfa_name = "a" + str(self.config["exp_id"]) + "_c" + str(cc) + "_r" + str(rc)
                dfb_name = "b" + str(self.config["exp_id"]) + "_c" + str(cc) + "_r" + str(rc)
                #print(type(json.dumps(changes)))
                ss = json.dumps(changes)
                dt_changes[dfa_name] = [self.config["exp_id"],dfa_name,rc,dfb_name,ss]

                #self.save_to_db(dfa, dfa_name)
                #self.save_to_db(dfb, dfb_name)
        self.tb_name_update(dt_changes)
                # self.save_to_excel(dfa,"T2.xlsx",dfa_name)
                # self.save_to_excel(dfb,"T2.xlsx",dfb_name)

    def exp_delete(self):
        query = ""
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
        self.tb_name_update("delete")

    def validate_schema(self, path):
        pass

    def get_df_schema(self, is_absolute=True, icount=1, fcount=0, scount=0, total=1) -> dict:
        """ isAbsolute - icount,fcount,scount contain absolute numbers or percents from total
            icount - number of integer columns
            fcount - number of float columns
            scount - number of string columns
            total - total number of elements in the result dataframe. If isAbsolute is True, total is ignoring
        """
        df_schema = {}
        if is_absolute == "False":
            if (icount + fcount + scount) != 100:
                raise CDCGeneratorException('Total percentage should be 100%')
            icount = int(total * (icount / 100))
            fcount = int(total * (fcount / 100))
            scount = int(total * (scount / 100))
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

    def get_df(self, rows_count=1, char_count=3) -> pd.DataFrame:
        """ df_schema - dict in form {"<column name>":<column value>}
            rows_count - number of rows in the Dataframe
            char_count - number of chars in the strings
        """
        df = pd.DataFrame(self.df_schema, index=[])
        for cname, ctype in self.df_schema.items():
            if type(ctype) == int:
                df[cname] = pd.Series(np.random.randint(0, 100000, size=rows_count))
            elif type(ctype) == float:
                df[cname] = pd.Series(np.random.rand(rows_count))
            elif type(ctype) == str:
                df[cname] = pd.Series(pd.util.testing.rands_array(char_count, rows_count))
        #df["old"] = ""
        return df

    def make_df_changes(self, dataframe, is_absolute=True, ucount=1, dcount=0, icount=0, total=1):
        """
            dataframe - pandas DataFrame, that will be changed
            isAbsolut - ucount,dcount,icount contain absolute numbers or percents from total. If isAbsolute is True,
            total is ignoring
            ucount - total number of updates
            dcount - total number of deletes
            icount - total number of inserts
        """
        dataframe = dataframe.copy()
        if is_absolute is False:
            if icount + dcount + ucount != 100:
                raise CDCGeneratorException('Total percentage should be 100%')
            icount = int(total * (icount / 100))
            ucount = int(total * (dcount / 100))
            dcount = int(total * (ucount / 100))

        len_df = len(dataframe)
        clms = list(dataframe.columns)
        # make updates
        update_indexes = np.random.randint(0, len_df, size=ucount)  # get indexes of rows to make inserts
        #[{"u":[{"id":0,}]}]
        # changes = [{"op":"u","id":0,"v":1}, {"op":"d","id":1}, {"op":"i","id":2,"v":"json"}]
        # [{'op': 'u', 'id': 5, 'v': '{"i0":92055,"f0":0.3052612745,"s0":"oua"}'}, {'op': 'd', 'id': 0}, {'op': 'd', 'id': 10, 'v': '{"i0":53718,"f0":0.0532709629,"s0":"dGK"}'}]
        changes = []
        for i in update_indexes:
            """ for dynamic changes generating
            old = dataframe["old"].iloc[i]
            dft = dataframe[clms].iloc[i].to_json()
            if len(old) == 0:
                old += dft
            else:
                old += "," + dft
            dataframe.loc[i, "old"] = old
            """
            val = json.loads(dataframe[clms].iloc[i].to_json())
            changes.append({"op":"u","id":int(i),"v":val})
            dataframe.iloc[i] = self.get_df(rows_count=1).iloc[0]

        # make deletes
        delete_indexes = list(np.random.randint(0, len_df, size=dcount))
        dataframe.drop(delete_indexes, inplace=True)
        for i in delete_indexes:
            changes.append({"op":"d","id":int(i)})
            # нужно ли сохранять что было удалено?
        # make inserts
        first_index = dataframe.tail(1).index.item()
        for i in range(first_index + 1, first_index + icount + 1):
            rnd_val = self.get_df(rows_count=1).iloc[0]  # get new random element with 1 element
            dataframe[i] = rnd_val
            changes.append({"op":"i","id":int(i),"v":json.loads(rnd_val.to_json())})
        return (dataframe,changes)

    def tb_name_update(self,dt_changes=None,op="append"):
        if op=="append" and dt_changes is not None:
            clms=["experiment_id","tb_source","elements","tb_target","changes"]
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


    def save_to_db(self, dataframe: pd.DataFrame, tb_name):
        try:
            connection_string = self.config["db_cs"]
            conn = create_engine(connection_string)
            dataframe.to_sql(tb_name, conn, if_exists='replace')
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
