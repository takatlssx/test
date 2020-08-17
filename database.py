#!/usr/bin/env python3
# coding: utf-8

import json
import sqlite3
import os
import datetime
import shutil

class DataBase:

    error = ""
    msg = ""
    root_dir = "/Users/taka/pg/database"

    def __init__(self,db_name):
        self.conn = None
        self.cursor = None
        self.setting = self.get_setting(db_name)
        if self.setting != False:
            self.open(self.setting["db_path"])

    def __del__(self):
        self.close()

    def get_setting(self,db_name):
        setting_path = self.root_dir + "/" + db_name + "/setting.json"
        if not os.path.exists(setting_path):
            self.error +=f"データベース設定取得エラー:\nデータベース設定ファイル{setting_path}が見つかりませんでした。 << DataBase.get_setting()\n"
            return False
        try:
            jsn = open(setting_path,"r")
            return json.load(jsn)
        except Exception as ex:
            self.error +="データベース設定取得エラー:\nデータベース設定を取得できませんでした。 << DataBase.get_setting()\n"+str(ex)
            return False

    def open(self,db_path):
        if not os.path.exists(db_path):
            self.error +=f"openエラー:\nデータベース:{db_path}が見つかりませんでした。 << DataBase.open()\n"
            return False
        try:
            self.conn = sqlite3.connect(db_path)
            self.cursor = self.conn.cursor()
            return True
        except Exception as ex:
            self.error +="openエラー:\nデータベースを開けませんでした。 << DataBase.open()\n"+str(ex)
            return False
    
    def close(self):
        if self.conn != None:
            self.conn.close()

    def is_exist_table(self,table_name):
        sql_str = "select count(*) from sqlite_master where type = 'table' and name = '{}'".format(table_name)
        self.cursor.execute(sql_str)
        if self.cursor.fetchone()[0] == 0:
            self.error +="table名エラー:\n{}というテーブルは存在しません。 << DataBase.is_exist_table()\n".format(table_name)
            return False
        else:
            return True

    def is_exist_columns(self,table_name,cols):
        if not self.is_exist_table(table_name):
            self.error += "<< DataBase.is_exist_colmuns()\n"
            return False
        columns = []
        try:
            self.cursor.execute("select * from "+table_name)
            columns = [description[0] for description in self.cursor.description]
            is_exist = True
            for c in cols:
                if c not in columns:
                    is_exist = False
                    self.error += "列名確認エラー:{}テーブルに{}という列は存在しません。<< DataBase.is_exist_columns()\n".format(table_name,c)
            return is_exist
        except Exception as ex:
            self.error += "列名確認エラー:<< DataBase.is_exist_columns()\n"+str(ex)
            return False

    def connect(self,db_path):
        if not os.path.exists(db_path):
            self.error += "接続するデータベース:"+db_path+"は存在しません。<< DataBase.connect()"
            return None
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        sql_str = "select count(*) from "+self.setting["main_table"]
        cursor.execute(sql_str)
        result = cursor.fetchall()
        self.main_table_data_cnt = result[0][0]
        cursor.close()
        return conn
    
    def get_data_count(self,table_name):
        sql_str = "select count(*) from "+table_name
        self.cursor.execute(sql_str)
        result = self.cursor.fetchall()
        return result[0][0]

    def get_data(self,table_name,cols=["*"]):
        if not self.is_exist_table(table_name):
            self.error += "データ取得エラー << DataBase.get_data()\n"
            return False
        if cols[0] != "*":
            if not self.is_exist_columns(table_name,cols):
                self.error += "データ取得エラー << DataBase.get_data()\n"
                return False
        try:
            cols_str = ",".join(cols).rstrip(",")
            sql_str = "select {} from {}".format(cols_str,table_name)
            self.cursor.execute(sql_str)
            return self.cursor.fetchall()
        except Exception as ex:
            self.error += "データ取得エラー: << DataBase.get_data()\n"+str(ex)
            return False

    def get_all_data_from_all_tables(self):
        data = {}
        try:
            for tbl_name in self.setting["tables"]:
                sql_str = "select * from {}".format(tbl_name)
                self.cursor.execute(sql_str)
                data[tbl_name] = self.cursor.fetchall()
            return data
        except Exception as ex:
            self.error += "データ取得エラー: << DataBase.get_all_data_from_all_tables()\n"+str(ex)
            return False

    def get_page_data(self,table_name,page,one_page_data_num,cols = ["*"]):
        if not self.is_exist_table(table_name):
            self.error += "データ取得エラー << DataBase.get_page_data()\n"
            return False
        if cols[0] != "*":
            if not self.is_exist_columns(table_name,cols):
                self.error += "データ取得エラー << DataBase.get_page_data()\n"
                return False
        all_page = (self.get_data_count(table_name) // one_page_data_num)
        if (self.get_data_count(table_name) % one_page_data_num) > 0:
            all_page += 1
        if page > all_page:
            self.error += f"データ取得エラー << DataBase.get_page_data()\n指定されたページ数:{page}はページ範囲:{all_page}を超えた数です。\n"
            return False
        try:
            cols_str = ",".join(cols).rstrip(",")
            sql_str = f"select {cols_str} from {table_name} limit {str((page-1)*one_page_data_num)},{str(one_page_data_num)}"
            self.cursor.execute(sql_str)
            return self.cursor.fetchall()
        except Exception as ex:
            self.error += "データが取得できませんでした。 << DataBase.get_page_data()\n"+str(ex)
            return False

    def select_by_id(self,id,table_name):
        if type(id) != int:
            self.error += "引数idはint型の値を指定してください。 << DataBase.select_by_id()"
            return False
        if not self.is_exist_table(table_name):
            self.error += "<< DataBase.select_by_id()\n"
            return False
        try:
            sql_str = f"select * from {table_name} where id = {id}"
            self.cursor.execute(sql_str)
            result = self.cursor.fetchone()
            if result == None:
                self.error += f"データが取得できませんでした。 << DataBase.select_by_id()\n{id}は存在しないidです。"
            return result
        except Exception as ex:
            self.error += "データが取得できませんでした。 << DataBase.select_by_id()\n"+str(ex)
            return False

    def create_sql_select(self,table_name,select_cols,words,and_or,match,view_cols=["*"]):
        cols_str = ",".join(view_cols).rstrip(",")
        sql_str = f"select {cols_str} from {table_name} where "
        try:
            for i,col in enumerate(select_cols):
                if col == "":
                    cond = "("
                    for cl in self.setting[table_name]["cols"]:
                        if match == "partial":
                            cond += "{} like '%{}%' or ".format(cl,words[i])
                        elif match == "perfect":
                            cond += "{} = '{}' or ".format(cl,words[i])
                    cond = cond.rstrip(" or ")
                    cond += ")"
                    sql_str += cond
                else:
                    if match == "partial":
                        sql_str += "({} like '%{}%')".format(col,words[i])
                    elif match == "perfect":
                        sql_str += "({} = '{}')".format(col,words[i])
                sql_str += and_or
            sql_str = sql_str.rstrip(and_or)
        except Exception as ex:
            self.error += "select_sqlの作成に失敗しました。 << DataBase.create_sql_select()\n"+str(ex)
            return False
        return sql_str

    def select(self,table_name,select_cols,words,and_or="or",match="partial",view_cols=["*"]):
        if not self.is_exist_table(table_name):
            self.error += "<< DataBase.select()\n"
            return False
        for cl in select_cols:
            if cl != "":
                if not self.is_exist_columns(table_name,[cl]):
                    self.error += "データセレクトエラー << DataBase.select()\n"
                    return False
        if view_cols[0] != "*":
            if not self.is_exist_columns(table_name,view_cols):
                self.error += "データセレクトエラー << DataBase.select()\n"
                return False
        if len(select_cols) != len(words):
            self.error += "データセレクトエラー:\n指定された列リストとワードリストの要素数が一致しません。 << DataBase.select()"
            return False
        if and_or != "and" and and_or != "or":
            self.error += f"データセレクトエラー:\n組み合わせ演算子はandかorのみ有効です{and_or}は無効な演算子です。<<DataBase.select()"
            return False
        if match != "perfect" and match != "partial":
            self.error += f"データセレクトエラー:\n一致方式はpartialかperfectのみ有効です{match}は無効な方式です。<<DataBase.select()"
            return False

        sql_str = self.create_sql_select(table_name,select_cols,words,and_or,match,view_cols)
        if not sql_str:
            return False
        try:
            self.cursor.execute(sql_str)
            return self.cursor.fetchall()
        except Exception as ex:
            self.error += f"データセレクトエラー:\n{table_name}テーブルからのデータセレクトに失敗しました。 << DataBase.select()\n"+str(ex)
            return False

    def validate(self,table_name,cols,new_data):
        if not self.is_exist_table(table_name):
            self.error += "validateエラー << DataBase.validate()\n"
            return False
        if not self.is_exist_columns(table_name,cols):
            self.error += "validateエラー << DataBase.validate()\n"
            return False
        if len(new_data) != len(cols):
            self.error += f"validateエラー << DataBase.validate()\nデータ数{len(new_data)}が、列数{len(cols)}と一致しません。"
            return False
        err = ""
        for i,c in enumerate(cols):
            if c == "id":
                if new_data[i] != None and new_data[i] != "":
                    try:
                        num = int(new_data[i])
                    except:
                        err += "typeエラー:id列は整数型のデータです。\n"
            if self.setting[table_name]["type"][c] == "integer" and c != "id":
                try:
                    num = int(new_data[i])
                except:
                    err += "typeエラー:{}列は整数型のデータです。\n".format(c)
            if self.setting[table_name]["type"][c] == "date":
                try:
                    num = datetime.datetime.strptime(new_data[i],"%Y-%m-%d")
                except:
                    err += "typeエラー:{}列は日付型(yyyy-mm-dd)のデータです。\n".format(c)
        for i,c in enumerate(cols):
            if c == "id":
                if new_data[i] == "":
                    new_data[i] = None
            if self.setting[table_name]["empty"][c] == "not_null" and (new_data[i] == "" or new_data[i] == None) and c != "id":
                err += "nullエラー:{}列はnullは禁止です。\n".format(c)
        if err != "":
            self.error += err
            return False
        else:
            return True

    def back_up(self):
        try:
            now = datetime.datetime.now()
            backup_file = self.setting["backup_path"] + self.setting["name"]+"{0:%Y%m%d%H%M%S}.backup".format(now)
            shutil.copyfile(self.setting["db_path"], backup_file)
            return True
        except Exception as ex:
            self.error += "dbファイルのバックアップに失敗しました。 << DataBase.back_up()\n"+str(ex)
            return False

    #途中
    def update(self,table_name,id,cols,new_data):
        
        if not self.validate(table_name,cols,new_data):
            self.error += " << DataBase.update()"
            return False
        old_data = self.select_by_id(id,table_name)
        if not old_data:
            self.error += " << DataBase.update()"
            return False
        try:
            sql_str = "update {} set {} where id = {}"
            set_str = ""
            for i in range(len(cols)):
                set_str += "{} = '{}',".format(cols[i],new_data[i])
            sql_str = sql_str.format(table_name,set_str.rstrip(","),str(id))
            self.cursor.execute(sql_str)
            self.conn.commit()
            self.msg += "{}テーブルのid:{}のデータを以下の通り更新しました。\n".format(table_name,str(id))
            for i,c in enumerate(cols):
                self.msg += "列{} : {} → {}\n".format(c,old_data[self.setting[table_name]["cols"].index(c)],new_data[i])
        except Exception as ex:
            self.error += "updateエラー:\n"+table_name+"テーブルのデータ更新に失敗しました。<<DataBase.update()\n"+str(ex)
            return False
        return True,old_data  

    #途中
    def regist(self,table_name,new_data):
        if not self.validate(table_name,self.setting[table_name]["cols"],new_data):
            self.error += " << DataBase.regist()"
            return False
        try:
            self.cursor.execute(self.setting[table_name]["insert_sql"],new_data)
            self.conn.commit()
            self.msg += table_name+"テーブルに以下のデータを登録しました。\n"
            for i in range(len(new_data)):
               self.msg += "{} = {}\n".format(self.setting[table_name]["cols"][i],str(new_data[i]))
        except Exception as ex:
            self.error += f"registエラー：{table_name}テーブルのデータ登録に失敗しました。<<DataBase.regist()\n"+str(ex)
            return False
        return True

    def update_transaction(self,table_name,id,cols,new_data):

        if not self.back_up():
            self.error += " << DataBase.update()"
            return False

        result = self.update(table_name,id,cols,new_data)
        if not result:
            return False
        
        for c in cols:
            if c in self.setting["tables"]:
                ds = new_data[cols.index(c)].split("/")
                for d in ds:
                    res = self.select(c,[c],[d],"or","perfect")
                    if len(res) == 0:
                        if not self.regist(c,[None,d]):
                            return False
        return True

        
