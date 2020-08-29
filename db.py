#!/usr/bin/env python3
# coding: utf-8

import json
import sqlite3
import os
import datetime
import shutil

class DB:
#constructer/destructer##################################################################
    _root_dir = "/Users/taka/database/"
    def __init__(self,name):
        #message
        self.error = ""
        self.msg = ""
        #base_item
        self.db_name = name
        self.setting_path = self._root_dir + name + "/setting.json"
        self.latest_backup_db_file = ""
        self.latest_backup_setting_file = ""
        #connection_object
        self.conn = None
        self.cursor = None
        #setting
        self.info = {}
        #connection(get_setting → connect)
        self.connect()
        
    def __del__(self):
        self.close()
   
#connect/close/get_setting###############################################################
    def connect(self):
        #dbファイルがなければ初期状態なので作成
        if not os.path.exists(self._root_dir + self.db_name + "/" + self.db_name + ".db"):
            self.create_db(self.db_name)
        else:
            try:
                self.get_setting()
                self.conn = sqlite3.connect(self.info["db_path"])
                self.cursor = self.conn.cursor()
                return True
            except Exception as ex:
                self.error +="connectエラー:<<DB.connect()\nデータベースを開けませんでした\n"+str(ex)
                return False
    
    def close(self):
        if self.conn != None:
            self.conn.close()
    
    def get_setting(self):
        if not os.path.exists(self.setting_path):
            self.error +=f"データベース設定取得エラー:<<DB.get_setting()\nデータベース設定ファイル{self.setting_path}が見つかりませんでした。"
            return False
        try:
            with open(self.setting_path,"r",encoding="utf-8") as f:
                self.info = json.load(f)
        except Exception as ex:
            self.error +="データベース設定取得エラー:<<DB.get_setting()\nデータベース設定を取得できませんでした。\n"+str(ex)
            return False

#create##################################################################################
    def create_db(self,db_name):
        try:
            db_dir = self._root_dir + db_name
            if not os.path.isdir(db_dir):
                os.makedirs(db_dir)
                os.makedirs(db_dir + "/backup")
                os.makedirs(db_dir + "/csv")
                os.makedirs(db_dir + "/image")
            self.conn = sqlite3.connect(db_dir + "/" + self.db_name + ".db")
            
            self.info["db_name"] = self.db_name
            self.info["db_path"] = db_dir+"/"+self.db_name+".db"
            self.info["backup_dir"] = db_dir+"/backup/"
            self.info["csv_dir"] = db_dir+"/csv/"
            self.info["image_dir"] = db_dir+"/image/"
            self.info["tables"] = []
            self.info["main_table"] = ""
            self.info["sub_tables"] = []
            self.info["relational_tables"] = []
            jsn_str = json.dumps(self.info,ensure_ascii=False,indent=4)
            with open(self.setting_path,"w",encoding="utf-8") as f:
                f.write(jsn_str)

            return True
        except Exception as ex:
            self.error += "データベース作成エラー:DB.create_db()\nデータベース/setting.jsonの作成に失敗しました。"+str(ex)
            return False

    def create_main_table(self,table_name,col_list,view_name_list,type_list,empty_list,relational_col_list,primary_key,autoincrement=True):
        #すでに存在するテーブル名かチェック
        sql_str = f"select count(*) from sqlite_master where type = 'table' and name = '{table_name}'"
        self.cursor.execute(sql_str)
        if self.cursor.fetchone()[0] != 0:
            self.error += f"create tableエラー:<<DB.create_main_table()\n{table_name}テーブルはすでに存在します。\n"
            return False
        #status引数がmainかsubかチェック
        if self.info["main_table"] != "" and self.info["main_table"] != None:
            self.error += f"create tableエラー:<<DB.create_main_table()\nすでにmainテーブルが設定されています、mainテーブルはdbに１つのみです。\n"
            return False
        #列数と型数が一致しているかチェック
        if not(len(col_list) == len(type_list) == len(view_name_list) == len(empty_list)):
            self.error += f"create tableエラー:<<DB.create_main_table()\n列数、型数、表示列名数、null許容数が異なります。\n"
            return False
        #型が決められた型かチェック
        tps = ["integer","text","date"]
        for tp in type_list:
            if tp not in tps:
                self.error += f"create tableエラー:<<DB.create_main_table()\n型{tp}は無効な型名です。有効な値は'integer' or 'text' or 'date'です。\n"
                return False
        #empty_listが正しいデータかチェック(null or not_null)
        for emp in empty_list:
            if emp != "null" and emp != "not_null":
                self.error += f"create tableエラー:<<DB.create_main_table()\nempty_list内の値{emp}は無効な値です。有効な値は'null' or 'not_null'です。\n"
                return False
        #relational_col_listチェック
        if relational_col_list != []:
            for rc in relational_col_list:
                if rc not in col_list:
                    self.error += f"create tableエラー:<<DB.create_main_table()\nリレーショナル列{rc}は列リストに存在しません。\n"
                    return False
        #primary_keyがcol_listにあるかチェック
        if primary_key not in col_list:
            self.error += f"create tableエラー:<<DB.create_main_table()\nプライマリーキー{primary_key}は列リストに存在しません。\n"
            return False
        #autoincrementのチェック
        if autoincrement and type_list[col_list.index(primary_key)] != "integer":
            self.error += f"create tableエラー:<<DB.create_main_table()\nプライマリーキー{primary_key}にインクリメントを指定する場合はinteger型を指定してください。\n"
            return False

        col_str = ""
        for i,col in enumerate(col_list):
            if col == primary_key:
                if autoincrement:
                    col_str += f"{col} {type_list[i]} primary key autoincrement,"
                else:
                    col_str += f"{col} {type_list[i]} primary key,"
            else:
                col_str += f"{col} {type_list[i]},"
        
        sql_str = f"create table {table_name}({col_str.rstrip(',')})"
        try:
            #バックアップ
            if not self.backup():
                self.error += "<<DB.create_main_table()"
                return False

            #メインテーブル作成
            self.cursor.execute(sql_str)
            self.conn.commit()
            self.info["tables"].append(table_name)
            self.info["main_table"] = table_name
            self.info[table_name] = {}
            self.info[table_name]["create_sql"] = sql_str
            self.info[table_name]["cols"] = col_list
            self.info[table_name]["primary_key"] = primary_key
            self.info[table_name]["autoincrement"] = autoincrement
            for i,col in enumerate(col_list):
                self.info[table_name][col] = {"view_name":view_name_list[i],"type":type_list[i],"empty":empty_list[i]}
            self.msg += f"{table_name}テーブル(メインテーブル)を作成しました。\n"

            #リレーショナルテーブル作成
            if relational_col_list != []:
                for relational_table in relational_col_list:
                    sql_str = f"create table {relational_table}(id integer primary key autoincrement,{relational_table} text)"
                    self.cursor.execute(sql_str)
                    self.conn.commit()
                    self.info["tables"].append(relational_table)
                    self.info["sub_tables"].append(relational_table)
                    self.info["relational_tables"].append(relational_table)
                    self.info[relational_table]["create_sql"] = sql_str
                    self.info[relational_table]["cols"] = ["id",relational_table]
                    self.info[relational_table]["primary_key"] = "id"
                    self.info[relational_table]["autoincrement"] = True
                    self.info[relational_table]["id"] = {"view_name":"管理番号","type":"integer","empty":"not_null"}
                    self.info[relational_table][relational_table] = {"view_name":view_name_list[col_list.index(relational_table)],"type":"text","empty":"not_null"}
                    self.msg += f"{relational_table}テーブル(サブ・リレーショナルテーブル)を作成しました。\n"
            #setting.json保存
            jsn_str = json.dumps(self.info_all,ensure_ascii=False,indent=4)
            with open(self.setting_path,"w",encoding="utf-8") as f:
                f.write(jsn_str)
            return True
        except Exception as ex:
            self.error += f"create tableエラー:<<DB.create_main_table()\ntable作成に失敗しました。\n{str(ex)}"
            return False
        
    def create_sub_table(self,table_name,col_list,view_name_list,type_list,empty_list,relational_col_list,primary_key,autoincrement=True):
        #すでに存在するテーブル名かチェック
        sql_str = f"select count(*) from sqlite_master where type = 'table' and name = '{table_name}'"
        self.cursor.execute(sql_str)
        if self.cursor.fetchone()[0] != 0:
            self.error += f"create tableエラー:<<DB.create_sub_table()\n{table_name}テーブルはすでに存在します。\n"
            return False
        #列数と型数が一致しているかチェック
        if not(len(col_list) == len(type_list) == len(view_name_list) == len(empty_list)):
            self.error += f"create tableエラー:<<DB.create_sub_table()\n列数、型数、表示列名数、null許容数が異なります。\n"
            return False
        #型が決められた型かチェック
        tps = ["integer","text","date"]
        for tp in type_list:
            if tp not in tps:
                self.error += f"create tableエラー:<<DB.create_sub_table()\n型{tp}は無効な型名です。有効な値は'integer' or 'text' or 'date'です。\n"
                return False
        #empty_listが正しいデータかチェック(null or not_null)
        for emp in empty_list:
            if emp != "null" and emp != "not_null":
                self.error += f"create tableエラー:<<DB.create_sub_table()\nempty_list内の値{emp}は無効な値です。有効な値は'null' or 'not_null'です。\n"
                return False
        #primary_keyがcol_listにあるかチェック
        if primary_key not in col_list:
            self.error += f"create tableエラー:<<DB.create_sub_table()\nプライマリーキー{primary_key}は列リストに存在しません。\n"
            return False
        #autoincrementのチェック
        if autoincrement and type_list[col_list.index(primary_key)] != "integer":
            self.error += f"create tableエラー:<<DB.create_sub_table()\nプライマリーキー{primary_key}にインクリメントを指定する場合はinteger型を指定してください。\n"
            return False

        col_str = ""
        for i,col in enumerate(col_list):
            if col == primary_key:
                if autoincrement:
                    col_str += f"{col} {type_list[i]} primary key autoincrement,"
                else:
                    col_str += f"{col} {type_list[i]} primary key,"
            else:
                col_str += f"{col} {type_list[i]},"
        
        sql_str = f"create table {table_name}({col_str.rstrip(',')})"
         try:
            #バックアップ
            if not self.backup():
                self.error += "<<DB.create_main_table()"
                return False

            #テーブル作成
            self.cursor.execute(sql_str)
            self.conn.commit()
            self.info["tables"].append(table_name)
            self.info["sub_tables"].append(table_name)
            self.info[table_name] = {}
            self.info[table_name]["create_sql"] = sql_str
            self.info[table_name]["cols"] = col_list
            for i,col in enumerate(col_list):
                self.info[table_name][col] = {"view_name":view_name_list[i],"type":type_list[i],"empty":empty_list[i]}
            self.msg += f"{table_name}テーブル(サブテーブル)を作成しました。\n"
            
            #setting.json保存
            jsn_str = json.dumps(self.info_all,ensure_ascii=False,indent=4)
            with open(self.setting_path,"w",encoding="utf-8") as f:
                f.write(jsn_str)
            return True
        except Exception as ex:
            self.error += f"create tableエラー:<<DB.create_sub_table()\n{table_name}テーブルの作成に失敗しました。\n{str(ex)}"
            return False
#check###################################################################################
    def is_exist_table(self,table_name):
        sql_str = f"select count(*) from sqlite_master where type = 'table' and name = '{table_name}'"
        self.cursor.execute(sql_str)
        if self.cursor.fetchone()[0] == 0:
            self.error +=f"table名エラー:<<DB.is_exist_table()\n{table_name}というテーブルは存在しません。\n"
            return False
        else:
            return True
    #途中
    def validate(self,table_name,data):
        if not self.is_exist_table(table_name):
            self.error += "データ検証エラー：<<DB.validate()\n"
            return False
        cols = self.info[table_name]["cols"]
        if len(cols) != len(new_data):
            self.error += f"データ検証エラー：<<DB.validate()\n新規データ数:{len(new_data)}がテーブル規定のデータ数:{len(cols)}と一致しません。\n"
            return False
        
        err = ""
        #type_check
        primary_key = self.info[table_name]["primary_key"]
        is_autoincrement = self.info[table_name]["autoincrement"]
        for i,col in enumerate(cols):
            if col == primary_key and is_increment:
                if new_data[i] != None and not isinstance(new_data[i],int):
                    err += f"typeエラー:プライマリー列{col}はNoneもしくはint型のデータを設定してください。\n"                
            else:
                if self.info[table_name][col]["type"] == "integer":
                    if not isinstance(new_data[i],int):
                        err += f"typeエラー:列{col}は整数型のデータを設定してください。\n"
                elif self.info[table_name][col]["type"] == "text":
                    if not isinstance(new_data[i],str):
                        err += f"typeエラー:列{col}は文字列型のデータを設定してください。\n"
                elif self.info[table_name][col]["type"] == "date":
                    pass
        #null_check
        for i,col in enumerate(cols):
            pass
#backup##################################################################################
    def backup(self):
        try:
            now = datetime.datetime.now()
            #.db
            self.latest_backup_db_file = self.info["backup_dir"] + self.db_name+"{0:%Y%m%d%H%M%S}.backup".format(now)
            shutil.copyfile(self.info["db_path"], self.latest_backup_db_file)

            #setting.json
            self.latest_backup_setting_file = self.info["backup_dir"] + "setting{0:%Y%m%d%H%M%S}.backup".format(now)
            shutil.copyfile(self.setting_path, self.latest_backup_setting_file)
            return True

        except Exception as ex:
            self.error += "dbファイル/settingファイルバックアップエラー:<<DB.back_up()\n"+str(ex)
            return False
        
#insert##########################################################################################
    def insert(self,table_name,new_data):
        if not self.is_exist_table(table_name):
            self.error += "<<DB.insert()"
            return False
        cols = self.info[table_name]["cols"]
        if len(cols) != len(new_data):
            self.error += f"insertエラー：<<DB.insert()\n新規データ数:{len(new_data)}が規定のデータ数:{len(cols)}と一致しません。\n"
            return False
