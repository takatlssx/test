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
   
#connect/close/setting###############################################################
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

    def save_setting(self):
        #現在のsetting.jsonをバックアップ
        try:
            dst = self.info["backup_dir"] + "setting{0:%Y%m%d%H%M%S}.backup".format(datetime.datetime.now())
            shutil.copyfile(self.setting_path, dst)
            self.latest_backup_setting_file = dst
        except Exception as ex:
            self.error += f"設定保存エラー：<< DataBase.save_setting()\nsetting.jsonをバックアップできませんでした。\n{str(ex)}\n"
            return False
        #現在のself.infoをsetting.jsonに書き込み
        try:
            setting_str = json.dumps(self.info,ensure_ascii=False,indent=4)
            with open(self.setting_path,"w",encoding="utf-8") as f:
                f.write(setting_str)
            return True
        except Exception as ex:
            self.error += f"設定保存エラー：<< DataBase.save_setting()\nsetting.jsonへの設定情報の書き込みに失敗しました。\n{str(ex)}\n"
            return False

#utility###################################################################################
    def get_data_count(self,table_name):
        if not self.is_exist_table(table_name):
            self.error += f"データ件数取得エラー:<<DB.get_data_count()\n{table_name}というテーブルは存在しません。\n"
            return False
        try:
            sql_str = "select count(*) from "+table_name
            self.cursor.execute(sql_str)
            result = self.cursor.fetchall()
            return result[0][0]
        except Exception as ex:
            self.error += f"データ件数取得エラー:<<DB.get_data_count()\n{table_name}テーブルからデータ件数を取得できませんでした。\n{str(ex)}\n"
            return False
    
    def get_columns(self,table_name):
        if self.is_exist_table(table_name):
            self.cursor.execute("select * from " + table_name)
            return [description[0] for description in self.cursor.description]
        else:
            self.error += f"{table_name}というテーブルは存在しません。<<DB.get_columns()\n"
            return False
    
    def copy_db(self,src_db,src_table_name,dest_table_name):
        src_db_data = None
        err = "データベースコピーエラー:<<DB.copy_db()\n"
        if not os.path.exists(src_db):
            self.error += f"{err}コピー元に指定されたデータベースファイル{src_db}が存在しません。\n"
            return False
        try:
            src_conn = sqlite.connection(src_db)
            src_cursor = src_conn.cursor()
            src_cursor.execute("select * from " + src_table_name)
            src_db_data = src_cursor.fetchall()
        except Exception as ex:
            self.error += f"{err}コピー元に指定されたデータベースファイル{src_db}からデータを取得できませんでした。\n"
            return False
        
        if len(src_db_data) == 0:
            self.error += f"{err}コピー元に指定されたデータベースファイル{src_db}にデータが存在しませんでした。\n"
            return False
        
        if self.insert_many(dest_table_name,src_db_data):
            return True
        else:
            self.error += f"{err}\n"
            return False
    
    def import_csv(self,csv_path,table_name):

        if not os.path.exists(csv_path):
            self.error += f"csvインポートエラー:<<DB.import_csv()\ncsv:{csv_path}が見つかりませんでした。\n"
            return False
        
        if not self.is_exist_table(table_name):
            self.error += f"csvインポートエラー:<<DB.import_csv()\n{table_name}テーブルは存在しません。\n"
            return False

        data_2d = []
        try:
            with open(csv_path,"r",encoding="utf-8") as f:
                for row in f:
                    data_2d.append(row.split(','))
        except Exception as ex:
            self.error += f"csvインポートエラー:<<DB.import_csv()\ncsvファイルをリストに変換できませんでした。\n{str(ex)}"
            return False

        if len(data_2d[0]) != len(self.info[table_name]["cols"]):
            self.error += f"csvインポートエラー:<<DB.import_csv()\ncsvファイルのデータ数と{table_name}テーブルのデータ数が一致しません。\n"
            return False
        
        #data_2dの最初のデータは列名リストなので削除する
        del data_2d[0]
        if not self.insert_many(table_name,data_2d):
            self.error += "<<import_csv()\n"
            return False
        
        return True

    #途中
    def export_csv(self,table_name):
        if not self.is_exist_table(table_name):
            self.error += f"csvエクスポートエラー:<<DB.export_csv()\n{table_name}テーブルは存在しません。\n"
            return False
        if not os.path.isdir(self.info[table_name]["csv_dir"]):
            self.error += f"csvエクスポートエラー:<<DB.export_csv()\ncsvフォルダ{self.info[table_name]['csv_dir']}が見つかりませんでした。\n"
            return False

#create###########################################################################################
    #データベースファイル作成メソッド
    def create_db(self,db_name):
        #データベースフォルダ作成
        try:
            db_dir = self._root_dir + db_name
            if not os.path.isdir(db_dir):
                os.makedirs(db_dir)
                os.makedirs(db_dir + "/backup")
                os.makedirs(db_dir + "/csv")
                os.makedirs(db_dir + "/image")
        except Exception as ex:
            self.error += f"データベース作成エラー:DB.create_db()\nデータベースフォルダの作成に失敗しました。\n{str(ex)}"
            return False
        #データベースに接続（自動で作成される）
        try:
            self.conn = sqlite3.connect(db_dir + "/" + self.db_name + ".db")
            self.cursor = self.conn.cursor()
        except:
            self.error += f"データベース作成エラー:DB.create_db()\nデータベースファイルの作成・接続に失敗しました。\n{str(ex)}"
            return False
        #設定情報を処理
        try:
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
        except Exception as ex:
            self.error += f"データベース作成エラー:DB.create_db()\nデータベース設定情報・setting.jsonの作成に失敗しました。{str(ex)}"
            return False
        return True

    def create_main_table(self,table_name,col_list,view_name_list,type_list,empty_list,relational_col_list,primary_key,autoincrement=True):
        #すでに存在するテーブル名かチェック
        if self.is_exist_table(table_name):
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

        #バックアップ
        if not self.backup():
            self.error += "<<DB.create_main_table()"
            return False

        #メインテーブル作成
        try:
            col_str = ""
            for i,col in enumerate(col_list):
                tp = type_list[i]
                if tp == "date":
                    tp = "text"
                if col == primary_key:
                    if autoincrement:
                        col_str += f"{col} {tp} primary key autoincrement,"
                    else:
                        col_str += f"{col} {tp} primary key,"
                else:
                    col_str += f"{col} {tp},"
            sql_str = f"create table {table_name}({col_str.rstrip(',')})"
            self.cursor.execute(sql_str)
            self.conn.commit()
        except Exception as ex:
            self.error += f"create tableエラー:<<DB.create_main_table()\nメインテーブル({table_name})の作成に失敗しました。\n{str(ex)}"
            return False
        #メインテーブル設定情報設定
        try:
            self.info["tables"].append(table_name)
            self.info["main_table"] = table_name
            self.info[table_name] = {}
            self.info[table_name]["create_sql"] = sql_str
            insert_sql_col_str = ",".join(["?" for i in range(len(col_list))])
            self.info[table_name]["insert_sql"] = f"insert into {table_name} values({insert_sql_col_str})"
            self.info[table_name]["cols"] = col_list
            self.info[table_name]["primary_key"] = primary_key
            self.info[table_name]["autoincrement"] = autoincrement
            for i,col in enumerate(col_list):
                self.info[table_name][col] = {"view_name":view_name_list[i],"type":type_list[i],"empty":empty_list[i]}
            self.msg += f"{table_name}テーブル(メインテーブル)を作成しました。\n"
        except Exception as ex:
            self.error += f"create tableエラー:<<DB.create_main_table()\nメインテーブル({table_name})の設定情報の設定に失敗しました。\n{str(ex)}"
            return False

        #リレーショナルテーブル作成
        if relational_col_list != []:
            for relational_table in relational_col_list:
                sql_str = f"create table {relational_table}(id integer primary key autoincrement,{relational_table} text)"
                try:
                    self.cursor.execute(sql_str)
                    self.conn.commit()
                except Exception as ex:
                    self.error += f"create tableエラー:<<DB.create_main_table()\nリレーショナルテーブル({relational_table})の作成に失敗しました。\n{str(ex)}"
                    return False
                try:
                    self.info["tables"].append(relational_table)
                    self.info["sub_tables"].append(relational_table)
                    self.info["relational_tables"].append(relational_table)
                    self.info[relational_table] = {}
                    self.info[relational_table]["create_sql"] = sql_str
                    self.info[relational_table]["insert_sql"] = f"insert into {relational_table} values(?,?)"
                    self.info[relational_table]["cols"] = ["id",relational_table]
                    self.info[relational_table]["primary_key"] = "id"
                    self.info[relational_table]["autoincrement"] = True
                    self.info[relational_table]["id"] = {"view_name":"管理番号","type":"integer","empty":"not_null"}
                    self.info[relational_table][relational_table] = {"view_name":view_name_list[col_list.index(relational_table)],"type":"text","empty":"not_null"}
                    self.msg += f"{relational_table}テーブル(サブ・リレーショナルテーブル)を作成しました。\n"
                except Exception as ex:
                    self.error += f"create tableエラー:<<DB.create_main_table()\nリレーショナルテーブル({relational_table})の設定情報の設定に失敗しました。\n{str(ex)}"
                    return False  
        #設定情報をsetting.jsonに書き込み
        if not self.save_setting():
            self.error += f"create tableエラー:<<DB.create_main_table()\n設定情報のsetting.jsonへの書き込みに失敗しました。\n"
            return False 
        
        return True
        
    def create_sub_table(self,table_name,col_list,view_name_list,type_list,empty_list,primary_key,autoincrement=True):
        #すでに存在するテーブル名かチェック
        if self.is_exist_table(table_name):
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
        
        #バックアップ
        if not self.backup():
            self.error += "<<DB.create_main_table()"
            return False
        #テーブル作成
        try:
            col_str = ""
            for i,col in enumerate(col_list):
                tp = type_list[i]
                if tp == "date":
                    tp = "text"
                if col == primary_key:
                    if autoincrement:
                        col_str += f"{col} {tp} primary key autoincrement,"
                    else:
                        col_str += f"{col} {tp} primary key,"
                else:
                    col_str += f"{col} {tp},"
            sql_str = f"create table {table_name}({col_str.rstrip(',')})"
            self.cursor.execute(sql_str)
            self.conn.commit()
        except Exception as ex:
            self.error += f"create tableエラー:<<DB.create_sub_table()\nサブテーブル({table_name})の作成に失敗しました。\n{str(ex)}"
            return False

        #設定情報処理
        try:
            self.info["tables"].append(table_name)
            self.info["sub_tables"].append(table_name)
            self.info[table_name] = {}
            self.info[table_name]["create_sql"] = sql_str
            insert_sql_col_str = ",".join(["?" for i in range(len(col_str))])
            self.info[table_name]["insert_sql"] = f"insert into {table_name} values({insert_sql_col_str})"
            self.info[table_name]["cols"] = col_list
            for i,col in enumerate(col_list):
                self.info[table_name][col] = {"view_name":view_name_list[i],"type":type_list[i],"empty":empty_list[i]}
            self.msg += f"{table_name}テーブル(サブテーブル)を作成しました。\n"
        except Exception as ex:
            self.error += f"create tableエラー:<<DB.create_sub_table()\n{table_name}テーブルの設定情報の設定に失敗しました。\n{str(ex)}"
            return False
        #設定情報をsetting.jsonに書き込み
        if not self.save_setting():
            self.error += f"create tableエラー:<<DB.create_sub_table()\n設定情報のsetting.jsonへの書き込みに失敗しました。\n"
            return False 

        return True
  
#check############################################################################################
    def is_exist_table(self,table_name):
        sql_str = f"select count(*) from sqlite_master where type = 'table' and name = '{table_name}'"
        self.cursor.execute(sql_str)
        if self.cursor.fetchone()[0] == 0:
            return False
        else:
            return True

    def is_exist_column(self,table_name,cols):
        if not self.is_exist_table(table_name):
            return False
        err = ""
        for col in cols:
            if col not in self.info[table_name]["cols"] and col != "*":
                err += f"列名エラー:'{col}'という列は存在しません。\n"
        if err != "":
            self.error += err + "<<DB.is_exist_column()\n"
            return False
        return True

    def validate(self,table_name,data):
        #テーブル名チェック
        if not self.is_exist_table(table_name):
            self.error +=f"データ検証エラー：<<DB.validate()\ntable名エラー:<<DB.is_exist_table()\n{table_name}というテーブルは存在しません。\n"
            return False
        #引数のデータがlist or tuple かチェック
        if not isinstance(data,tuple) and not isinstance(data,list):
            self.error += f"データ検証エラー：<<DB.validate()\n新規データはlist型もしくはtuple型データを指定してください。\n"
            return False
        #新規データリストのデータ数が正しい数かチェック
        cols = self.info[table_name]["cols"]
        if len(cols) != len(data):
            self.error += f"データ検証エラー：<<DB.validate()\n新規データ数:{len(data)}がテーブル規定のデータ数:{len(cols)}と一致しません。\n"
            return False
        
        err = "データ検証エラー：<<DB.validate()\n"
        #type_check
        primary_key = self.info[table_name]["primary_key"]
        is_autoincrement = self.info[table_name]["autoincrement"]
        for i,col in enumerate(cols):
            if col == primary_key and is_autoincrement:
                if data[i] != None and not isinstance(data[i],int):
                    err += f"typeエラー:プライマリー列{col}はNoneもしくはint型のデータを設定してください。\n"                
            else:
                if self.info[table_name][col]["type"] == "integer":
                    try:
                        num = int(data[i])
                    except Exception as ex:
                        err += f"typeエラー:列{col}は整数型のデータを設定してください。\n"
                elif self.info[table_name][col]["type"] == "text":
                    if not isinstance(data[i],str):
                        err += f"typeエラー:列{col}は文字列型のデータを設定してください。\n"
                elif self.info[table_name][col]["type"] == "date":
                    try:
                        num = datetime.datetime.strptime(data[i],"%Y-%m-%d")
                    except:
                        err += f"typeエラー:列{col}は有効な日付型(yyyy-mm-dd)のデータを設定してください。\n"
        #null_check
        for i,col in enumerate(cols):
            if self.info[table_name][col]["empty"] == "not_null" and (data[i] == None or data[i] == "") and col != primary_key:
                err += f"nullエラー:列{col}はnullは禁止です。\n"
        
        if err != "データ検証エラー：<<DB.validate()\n":
            self.error += err
            return False
        else:
            return True

#backup/rollback##################################################################################
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

    def rollback(self):
        try:
            #db
            shutil.copy(self.latest_backup_db_file,self.info["db_path"])
            #setting.json
            shutil.copy(self.latest_backup_setting_file,self.setting_path)
        except Exception as ex:
            self.error += f"dbファイル/settingファイル・ロールバックエラー:<<DB.rollback()\n{str(ex)}"

#insert##########################################################################################
    def insert(self,table_name,new_data):
        if not self.validate(table_name,new_data):
            self.error += "データ登録エラー:<<DB.insert()\n"
            return False
        new_data = tuple(new_data)
        try:
            sql_str = self.info[table_name]["insert_sql"]
            self.cursor.execute(sql_str,new_data)
            self.conn.commit()
        except Exception as ex:
            self.error += f"データ登録エラー:<<DB.insert()\n新規データのinsertに失敗しました。\n{str(ex)}"
            return False
        self.msg += table_name+"テーブルに以下のデータを登録しました。\n"
        for i in range(len(new_data)):
            self.msg += f"{self.info[table_name]['cols'][i]} = {new_data[i]}\n"
        return True
    
    def insert_many(self,table_name,new_data_2d):
        if not self.backup():
            self.error += f"<<DB.insert_many()\n"
            return False
        for i,data in enumerate(new_data_2d):
            if not self.validate(table_name,data):
                self.error += "データ登録エラー:<<DB.insert()\n"
                return False
            data = tuple(data)
            try:
                sql_str = self.info[table_name]["insert_sql"]
                self.cursor.execute(sql_str,data)
            except Exception as ex:
                self.error += f"データ登録エラー:<<DB.insert_many()\n{i}個目の新規データのinsertに失敗しました。\n{str(ex)}"
                return False
        try:
            self.conn.commit()
            self.msg += f"{table_name}テーブルに{len(new_data_2d)}個のデータを登録しました。\n"
            return True
        except Exception as ex:
            self.rollback()
            self.error += f"データ登録エラー:<<DB.insert_many()\nデータベースのコミットに失敗しました。\n{str(ex)}"
            return False
    
    #途中
    def insert_main_table(self,new_data):
        main_table = self.info["main_table"]
        if not self.backup():
            self.error += f"<< DB.insert_main_table()\n"
            return False

        if not self.insert(self.info["main_table"],new_data):
            self.error += f"<< DB.insert_main_table()\n"
            return False

        #subtable
        for rltbl in self.info["relational_tables"]:
            vals = new_data[self.info[main_table]["cols"].index(rltbl)].split("/")
            for val in vals:
                pass

#select/get_data#################################################################################
    def get_data(self,table_name,select_cols):
        if not self.is_exist_table(table_name):
            self.error += f"データ取得エラー:<<DB.get_data()\n{table_name}というテーブルは存在しません。\n"
            return False
        try:
            sql_str = f"select {','.join(select_cols)} from {table_name}"
            self.cursor.execute(sql_str)
            return self.cursor.fetchall()
        except Exception as ex:
            self.error += f"データ取得エラー:<<DB.get_data()\n{table_name}テーブルからデータを取得できませんでした。\n{str(ex)}\n"
            return False
    
    #selectのsqlを作成メソッド
    def create_select_sql(self,table_name,select_cols,words,and_or,match,view_cols=["*"]):
        view_cols_str = ",".join(view_cols) if view_cols[0] != "*" else "*"
        sql_str = ""
        param = " like '%{}%'" if match == "partial" else " = '{}'"
        condition_list = []
        try:
            for i,col in enumerate(select_cols):
                if col == "":
                    lst = [val + param.format(words[i]) for val in self.setting[table_name]["cols"]]
                    condition_list.append(f"({' or '.join(lst)})")
                else:
                    condition_list.append(f"({col}{param.format(words[i])})")
            sql_str = f"select {view_cols_str} from {table_name} where {and_or.join(condition_list)}"
        except Exception as ex:
            self.error += "select_sql作成エラー:<< DB.create_sql_select()\n"+str(ex)
            return False
        return sql_str
    
    #selectメソッド
    def select(self,table_name,select_cols,words,and_or="or",match="partial",view_cols=["*"]):
        if not self.is_exist_table(table_name):
            self.error +=f"データセレクトエラー：<<DB.select()\ntable名エラー:<<DB.is_exist_table()\n{table_name}というテーブルは存在しません。\n"
            return False
        if not self.is_exist_column(table_name,select_cols):
            self.error +=f"データセレクトエラー：<<DB.select()\n"
            return False
        if not self.is_exist_column(table_name,view_cols):
            self.error +=f"データセレクトエラー：<<DB.select()\n"
            return False 
        if len(select_cols) != len(words):
            self.error += "データセレクトエラー:<<DB.select()\n指定された列リストとワードリストの要素数が一致しません。\n"
            return False
        if and_or != "and" and and_or != "or":
            self.error += f"データセレクトエラー:<<DB.select()\n組み合わせ演算子はandかorのみ有効です{and_or}は無効な演算子です。\n"
            return False
        if match != "perfect" and match != "partial":
            self.error += f"データセレクトエラー:<<DB.select()\n一致方式はpartialかperfectのみ有効です{match}は無効な方式です。\n"
            return False

        sql_str = self.create_select_sql(table_name,select_cols,words,and_or,match,view_cols)
        if not sql_str:
            return False
        try:
            self.cursor.execute(sql_str)
            return self.cursor.fetchall()
        except Exception as ex:
            self.error += f"データセレクトエラー:<<DB.select()\n{table_name}テーブルからのデータセレクトに失敗しました。\n"+str(ex)
            return False
    
    def select_by_id(self,table_name,id):
        if not self.is_exist_table(table_name):
            self.error += f"<<DB.select_by_id()\n{table_name}というテーブルは存在しません。\n"
            return False
        
        try:
            num = int(id)
        except Exception as ex:
            self.error += f"データセレクトエラー:<<DB.select_by_id()\nidは整数値を指定してください。\n"
            return False
        
        data = self.select(table_name,["id"],[id],"and","perfect")

        if data == False:
            self.error += f"データセレクトエラー:<<DB.select_by_id()\n"
            return False
        elif len(data) == 0:
            self.error += f"データセレクトエラー:<<DB.select_by_id()\nid'{id}'のデータは見つかりませんでした。\n"
            return False
        else:
            return data
