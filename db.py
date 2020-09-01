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
        #メッセージ文字列
        self.error = ""
        self.msg = ""
        #データベース名
        self.db_name = name
        #最近のバックアップファイルのパス
        self.latest_backup_db_file = ""
        self.latest_backup_setting_file = ""
        #コネクションオブジェクト
        self.conn = None
        self.cursor = None
        #設定関係
        self.setting_path = self._root_dir + name + "/setting.json"
        self.info = {}
        #コネクションメソッド実行(get_setting → connect)method
        self.connect()
        
    def __del__(self):
        self.close()
   
#connect/close/setting###############################################################
    #コネクションメソッド>>設定取得→コネクト
    def connect(self):
        #dbファイルがなければ初期状態なので作成
        if not os.path.exists(self._root_dir + self.db_name + "/" + self.db_name + ".db"):
            self.create_db(self.db_name)
        else:
            if not self.get_setting():
                self.error += "connectエラー:<<connect()\n"
                return False
            try:
                self.conn = sqlite3.connect(self.info["db_path"])
                self.cursor = self.conn.cursor()
                return True
            except Exception as ex:
                self.error +="connectエラー:<<connect()\nデータベースを開けませんでした\n"+str(ex)
                return False
    
    def close(self):
        if self.conn != None:
            self.conn.close()
    
    def get_setting(self):
        if not os.path.exists(self.setting_path):
            self.error +=f"データベース設定取得エラー:<<get_setting()\nデータベース設定ファイル{self.setting_path}が見つかりませんでした。"
            return False
        try:
            with open(self.setting_path,"r",encoding="utf-8") as f:
                self.info = json.load(f)
            return True
        except Exception as ex:
            self.error +="データベース設定取得エラー:<<get_setting()\nデータベース設定を取得できませんでした。\n"+str(ex)
            return False

    def save_setting(self):
        #現在のsetting.jsonをバックアップ
        try:
            dest = self.info["backup_dir"] + "setting{0:%Y%m%d%H%M%S}.backup".format(datetime.datetime.now())
            shutil.copyfile(self.setting_path, dest)
            self.latest_backup_setting_file = dest
        except Exception as ex:
            self.error += f"設定保存エラー：<< save_setting()\nsetting.jsonをバックアップできませんでした。\n{str(ex)}\n"
            return False
        #現在のself.infoをsetting.jsonに書き込み
        try:
            setting_str = json.dumps(self.info,ensure_ascii=False,indent=4)
            with open(self.setting_path,"w",encoding="utf-8") as f:
                f.write(setting_str)
            return True
        except Exception as ex:
            self.error += f"設定保存エラー：<< save_setting()\nsetting.jsonへの設定情報の書き込みに失敗しました。\n{str(ex)}\n"
            return False

#utility###################################################################################
    def get_data_count(self,table_name):
        if not self.is_exist_table(table_name):
            self.error += f"データ件数取得エラー:<<get_data_count()\n{table_name}というテーブルは存在しません。\n"
            return False
        try:
            sql_str = "select count(*) from "+table_name
            self.cursor.execute(sql_str)
            result = self.cursor.fetchall()
            return result[0][0]
        except Exception as ex:
            self.error += f"データ件数取得エラー:<<get_data_count()\n{table_name}テーブルからデータ件数を取得できませんでした。\n{str(ex)}\n"
            return False
    
    def get_columns(self,table_name):
        if self.is_exist_table(table_name):
            self.cursor.execute("select * from " + table_name)
            return [description[0] for description in self.cursor.description]
        else:
            self.error += f"列リスト取得エラー:<<get_columns()\n{table_name}というテーブルは存在しません\n"
            return False
    
    def copy_table(self,src_db,src_table_name,dest_table_name):
        src_db_data = None
        err = "データベースコピーエラー:<<copy_table()\n"
        if not os.path.exists(src_db):
            self.error += f"{err}コピー元に指定されたデータベースファイル{src_db}が存在しません。\n"
            return False
        try:
            src_conn = sqlite3.connect(src_db)
            src_cursor = src_conn.cursor()
            src_cursor.execute("select * from " + src_table_name)
            src_db_data = src_cursor.fetchall()
            src_conn.close()
        except Exception as ex:
            self.error += f"{err}コピー元に指定されたデータベースファイル{src_db}の{src_table_name}テーブルからデータを取得できませんでした。\n{str(ex)}n"
            return False
        
        if len(src_db_data) == 0:
            self.error += f"{err}コピー元に指定されたデータベースファイル{src_db}の{src_table_name}テーブルデータが存在しませんでした。\n"
            return False
        
        if self.insert_many(dest_table_name,src_db_data):
            self.msg += f"{src_table_name}テーブルを{dest_table_name}にコピーしました。\n"
            return True
        else:
            self.error += f"{err}\n"
            return False
    
    def import_csv(self,csv_path,table_name):
        err = "csvインポートエラー:<<import_csv()\n"
        if not os.path.exists(csv_path):
            self.error += f"{err}csv:{csv_path}が見つかりませんでした。\n"
            return False
        
        if not self.is_exist_table(table_name):
            self.error += f"{err}{table_name}テーブルは存在しません。\n"
            return False

        data_2d = []
        try:
            with open(csv_path,"r",encoding="utf-8") as f:
                for row in f:
                    data_2d.append(row.split(','))
        except Exception as ex:
            self.error += f"{err}csvファイルをリストに変換できませんでした。\n{str(ex)}"
            return False

        if len(data_2d[0]) != len(self.info[table_name]["cols"]):
            self.error += f"{err}csvファイルのデータ数と{table_name}テーブルのデータ数が一致しません。\n"
            return False
        
        #data_2dの最初のデータは列名リストなので削除する
        del data_2d[0]
        if not self.insert_many(table_name,data_2d):
            self.error += err
            return False
        
        return True

    def export_csv(self,table_name):
        err = "csvエクスポートエラー:<<export_csv()\n"
        if not self.is_exist_table(table_name):
            self.error += f"{err}{table_name}テーブルは存在しません。\n"
            return False
        if not os.path.isdir(self.info["csv_dir"]):
            self.error += f"{err}csvフォルダ{self.info[table_name]['csv_dir']}が見つかりませんでした。\n"
            return False
        
        data = []
        try:
            data = self.cursor.execute("select * from "+table_name).fetchall()
        except Exception as ex:
            self.error += f"{err}{table_name}テーブルのデータ取得に失敗しました。\n{str(ex)}\n"
            return False

        csv_str = ""
        csv_str += ",".join(self.info[table_name]["cols"]) + "\n"
        try:
            for row in data:
                csv_str += ",".join([str(val) for val in row]) + "\n"
        except Exception as ex:
            self.error += f"{err}データをcsv形式に変換できませんでした。\n{str(ex)}\n"
            return False
        
        try:
            dest = self.info["csv_dir"] + table_name + ".csv"
            with open(dest,"w",encoding="utf-8") as f:
                f.write(csv_str)
            self.msg += f"{table_name}テーブルのデータをcsvファイル{dest}に出力しました。\n"
            return True
        except Exception as ex:
            self.error += f"{err}データをcsvファイルに書き込みできませんでした。\n{str(ex)}\n"
            return False

#create###########################################################################################
    #データベースファイル作成メソッド
    def create_db(self,db_name):
        err = "データベース作成エラー:<<create_db()\n"
        #データベースフォルダ作成
        try:
            db_dir = self._root_dir + db_name
            if not os.path.isdir(db_dir):
                os.makedirs(db_dir)
                os.makedirs(db_dir + "/backup")
                os.makedirs(db_dir + "/csv")
                os.makedirs(db_dir + "/image")
        except Exception as ex:
            self.error += f"{err}データベースフォルダの作成に失敗しました。\n{str(ex)}"
            return False
        #データベースに接続（自動で作成される）
        try:
            self.conn = sqlite3.connect(db_dir + "/" + self.db_name + ".db")
            self.cursor = self.conn.cursor()
        except:
            self.error += f"{err}データベースファイルの作成・接続に失敗しました。\n{str(ex)}"
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
            self.error += f"{err}データベース設定情報・setting.jsonの作成に失敗しました。{str(ex)}"
            return False
        return True

    def create_main_table(self,table_name,col_list,view_name_list,type_list,empty_list,relational_col_list,primary_key,autoincrement=True):
        err = "メインテーブル作成エラー:<<create_main_table()\n"
        #すでに存在するテーブル名かチェック
        if self.is_exist_table(table_name):
            self.error += f"{err}{table_name}テーブルはすでに存在します。\n"
            return False
        #status引数がmainかsubかチェック
        if self.info["main_table"] != "" and self.info["main_table"] != None:
            self.error += f"{err}すでにmainテーブルが設定されています、mainテーブルはdbに１つのみです。\n"
            return False
        #列数と型数が一致しているかチェック
        if not(len(col_list) == len(type_list) == len(view_name_list) == len(empty_list)):
            self.error += f"{err}列数、型数、表示列名数、null許容数が異なります。\n"
            return False
        #型が決められた型かチェック
        tps = ["integer","text","date"]
        for tp in type_list:
            if tp not in tps:
                self.error += f"{err}型{tp}は無効な型名です。有効な値は'integer' or 'text' or 'date'です。\n"
                return False
        #empty_listが正しいデータかチェック(null or not_null)
        for emp in empty_list:
            if emp != "null" and emp != "not_null":
                self.error += f"{err}empty_list内の値{emp}は無効な値です。有効な値は'null' or 'not_null'です。\n"
                return False
        #relational_col_listチェック
        if relational_col_list != []:
            for rc in relational_col_list:
                if rc not in col_list:
                    self.error += f"{err}リレーショナル列{rc}は列リストに存在しません。\n"
                    return False
        #primary_keyがcol_listにあるかチェック
        if primary_key not in col_list:
            self.error += f"{err}プライマリーキー:{primary_key}は列リストに存在しません。\n"
            return False
        #autoincrementのチェック
        if autoincrement and type_list[col_list.index(primary_key)] != "integer":
            self.error += f"{err}プライマリーキー:{primary_key}にインクリメントを指定する場合はinteger型を指定してください。\n"
            return False

        #バックアップ
        if not self.backup("createmaintable"):
            self.error += err
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
            self.error += f"{err}メインテーブル:{table_name}の作成に失敗しました。\n{str(ex)}"
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
            self.error += f"{err}メインテーブル:{table_name}の設定情報の設定に失敗しました。\n{str(ex)}"
            return False

        #リレーショナルテーブル作成
        if relational_col_list != []:
            for relational_table in relational_col_list:
                sql_str = f"create table {relational_table}(id integer primary key autoincrement,{relational_table} text)"
                try:
                    self.cursor.execute(sql_str)
                    self.conn.commit()
                except Exception as ex:
                    self.error += f"{err}リレーショナルテーブル({relational_table})の作成に失敗しました。\n{str(ex)}"
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
                    self.error += f"{err}リレーショナルテーブル({relational_table})の設定情報の設定に失敗しました。\n{str(ex)}"
                    return False  
        #設定情報をsetting.jsonに書き込み
        if not self.save_setting():
            self.error += f"{err}設定情報のsetting.jsonへの書き込みに失敗しました。\n"
            return False 
        
        return True
        
    def create_sub_table(self,table_name,col_list,view_name_list,type_list,empty_list,primary_key,autoincrement=True):
        err = "サブテーブル作成エラー:<<create_sub_table()\n"
        #すでに存在するテーブル名かチェック
        if self.is_exist_table(table_name):
            self.error += f"{err}{table_name}テーブルはすでに存在します。\n"
            return False
        #列数と型数が一致しているかチェック
        if not(len(col_list) == len(type_list) == len(view_name_list) == len(empty_list)):
            self.error += f"{err}列数、型数、表示列名数、null許容数が異なります。\n"
            return False
        #型が決められた型かチェック
        tps = ["integer","text","date"]
        for tp in type_list:
            if tp not in tps:
                self.error += f"{err}型{tp}は無効な型名です。有効な値は'integer' or 'text' or 'date'です。\n"
                return False
        #empty_listが正しいデータかチェック(null or not_null)
        for emp in empty_list:
            if emp != "null" and emp != "not_null":
                self.error += f"{err}empty_list内の値{emp}は無効な値です。有効な値は'null' or 'not_null'です。\n"
                return False
        #primary_keyがcol_listにあるかチェック
        if primary_key not in col_list:
            self.error += f"{err}プライマリーキー{primary_key}は列リストに存在しません。\n"
            return False
        #autoincrementのチェック
        if autoincrement and type_list[col_list.index(primary_key)] != "integer":
            self.error += f"{err}プライマリーキー{primary_key}にインクリメントを指定する場合はinteger型を指定してください。\n"
            return False
        
        #バックアップ
        if not self.backup("createsubtable"):
            self.error += err
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
            self.error += f"{err}サブテーブル({table_name})の作成に失敗しました。\n{str(ex)}"
            return False

        #設定情報処理
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
            self.msg += f"{table_name}テーブル(サブテーブル)を作成しました。\n"
        except Exception as ex:
            self.error += f"{err}{table_name}テーブルの設定情報の設定に失敗しました。\n{str(ex)}"
            return False
        #設定情報をsetting.jsonに書き込み
        if not self.save_setting():
            self.error += f"{err}設定情報のsetting.jsonへの書き込みに失敗しました。\n"
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
        #col_list = self.get_columns(table_name)
        for col in cols:
            if col not in self.info[table_name]["cols"] and col != "*":
                err += f"列名エラー:'{col}'という列は存在しません。\n"
        if err != "":
            self.error += err + "<<is_exist_column()\n"
            return False
        return True

    def validate(self,table_name,data):
        err = "データ検証エラー：<<validate()\n"
        #テーブル名チェック
        if not self.is_exist_table(table_name):
            self.error +=f"{err}{table_name}というテーブルは存在しません。\n"
            return False
        #引数のデータがlist or tuple かチェック
        if not isinstance(data,tuple) and not isinstance(data,list):
            self.error += f"{err}新規データはlist型もしくはtuple型データを指定してください。\n"
            return False
        #データリストのデータ数が正しい数かチェック
        cols = self.info[table_name]["cols"]
        if len(cols) != len(data):
            self.error += f"{err}新規データ数:{len(data)}がテーブル規定のデータ数:{len(cols)}と一致しません。\n"
            return False
        
        #type_check
        primary_key = self.info[table_name]["primary_key"]
        is_autoincrement = self.info[table_name]["autoincrement"]
        for i,col in enumerate(cols):
            if col == primary_key and is_autoincrement:
                if data[i] != None:
                    try:
                        num = int(data[i])
                    except Exception as ex:
                        err += f"typeエラー:プライマリー列{col}はNoneもしくは整数型のデータを設定してください。\n"                
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
        
        if err != "データ検証エラー：<<validate()\n":
            self.error += err
            return False
        else:
            return True

#backup/rollback##################################################################################
    def backup(self,method=""):
        try:
            now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            #.db
            self.latest_backup_db_file = f"{self.info['backup_dir']}{self.db_name}{now}{method}.backup"
            shutil.copyfile(self.info["db_path"], self.latest_backup_db_file)

            #setting.json
            self.latest_backup_setting_file = f"{self.info['backup_dir']}setting{now}{method}.backup"
            shutil.copyfile(self.setting_path, self.latest_backup_setting_file)
            return True

        except Exception as ex:
            self.error += "db/settingバックアップエラー:<<backup()\n"+str(ex)
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
        err = "データ登録エラー:<<insert()\n"
        if not self.validate(table_name,new_data):
            self.error += err
            return False
        new_data = tuple(new_data)
        try:
            sql_str = self.info[table_name]["insert_sql"]
            self.cursor.execute(sql_str,new_data)
            self.conn.commit()
        except Exception as ex:
            self.error += f"{err}{table_name}テーブルへの新規データ登録に失敗しました。\n{str(ex)}"
            return False
        self.msg += table_name+"テーブルに以下のデータを登録しました。\n"
        for i in range(len(new_data)):
            self.msg += f"{self.info[table_name]['cols'][i]} = {new_data[i]}\n"
        return True
    
    def insert_many(self,table_name,new_data_2d):
        err = "データ登録エラー:<<insert_many()\n"
        if not self.backup("insertmany"+table_name):
            self.error += err
            return False
        for i,data in enumerate(new_data_2d):
            if not self.validate(table_name,data):
                self.error +=err
                return False
            data = tuple(data)
            try:
                sql_str = self.info[table_name]["insert_sql"]
                self.cursor.execute(sql_str,data)
            except Exception as ex:
                self.error += f"{err}{i}個目の新規データのinsertに失敗しました。\n{str(ex)}"
                return False
        try:
            self.conn.commit()
            self.msg += f"{table_name}テーブルに{len(new_data_2d)}個のデータを登録しました。\n"
            return True
        except Exception as ex:
            self.rollback()
            self.error += f"{err}データベースのコミットに失敗しました。\n{str(ex)}"
            return False
    
    #途中
    def insert_main_table(self,new_data):
        err = "メインテーブルデータ登録エラー:<<insert_main_table()\n"
        main_table_name = self.info["main_table"]
        
        #backup
        if not self.backup("insertmaintable"):
            self.error += err
            return False
        #maintable
        if not self.insert(main_table_name,new_data):
            self.rollback()
            self.error += err
            return False

        #subtable
        for rltbl in self.info["relational_tables"]:
            vals = new_data[self.info[main_table_name]["cols"].index(rltbl)].split("/")
            for val in vals:
                dt = self.select(rltbl,[rltbl],[val])
                if len(dt) == 0:
                    if not self.insert(rltbl,(None,val)):
                        self.error += err
                        return False
        return True

#update##########################################################################################
    def update(self,table_name,id,data):
        err = "データ変更エラー:<<update()\n"
        if not self.validate(table_name,data):
            self.error += err
            return False
        data = tuple(data)
        old_data = self.select_by_id(table_name,id)
        if not old_data:
            self.error += err
            return False
        
        change_col = []
        change_val = []
        old_val = []

        for i,od in enumerate(old_data):
            if str(od) != str(data[i]):
                change_col.append(self.info[table_name]["cols"][i])
                change_val.append(data[i])
                old_val.append(od)
        
        if len(change_val) == 0:
            self.error += f"{err}変更されたデータがありません。\n"
            return False
        
        set_str = ""
        self.msg = f"{table_name}テーブルのid:{id}のデータを以下の通り更新しました。\n"
        for i,col in enumerate(change_col):
            set_str += f"{col} = '{str(change_val[i])}',"
            self.msg += f"列:{col} '{old_val[i]}' >> '{change_val[i]}'\n"
        
        sql_str = f"update {table_name} set {set_str.rstrip(',')} where id = {id}"
        try:
            self.cursor.execute(sql_str)
            self.conn.commit()
            return True
        except Exception as ex:
            self.error += f"{err}{table_name}テーブルのid:{id}のデータ変更に失敗しました。\n{str(ex)}\n"
            return False

    def update_main_table(self,id,data):
        err = "メインテーブルデータ変更エラー:<<update_main_table()\n"
        if not self.backup("updatemaintable"):
            self.error += err
            return False
        main_table_name = self.info["main_table"]
        if not self.update(main_table_name,id,data):
            self.error += err
            return False
        
        #subtable
        for rltbl in self.info["relational_tables"]:
            vals = data[self.info[main_table_name]["cols"].index(rltbl)].split("/")
            for val in vals:
                dt = self.select(rltbl,[rltbl],[val])
                if len(dt) == 0:
                    if not self.insert(rltbl,(None,val)):
                        self.error += err
                        return False
        return True

#select/get_data#################################################################################
    def get_data(self,table_name,select_cols=["*"]):
        if not self.is_exist_table(table_name):
            self.error += f"データ取得エラー:<<get_data()\n{table_name}というテーブルは存在しません。\n"
            return False
        try:
            sql_str = f"select {','.join(select_cols)} from {table_name}"
            self.cursor.execute(sql_str)
            return self.cursor.fetchall()
        except Exception as ex:
            self.error += f"データ取得エラー:<<get_data()\n{table_name}テーブルからデータを取得できませんでした。\n{str(ex)}\n"
            return False
    
    def get_data_all_tables(self):
        data = {}
        for tbl in self.info["tables"]:
            data[tbl] = self.get_data(tbl)
            if not data[tbl]:
                self.error += "<<get_data_all_tables()\n"
                return False
        return data

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
            self.error += "select_sql作成エラー:<< create_sql_select()\n"+str(ex)
            return False
        return sql_str
    
    #selectメソッド
    def select(self,table_name,select_cols,words,and_or="or",match="partial",view_cols=["*"]):
        err = "データセレクトエラー：<<select()\n"
        if not self.is_exist_table(table_name):
            self.error +=f"{err}{table_name}というテーブルは存在しません。\n"
            return False
        if not self.is_exist_column(table_name,select_cols):
            self.error += err
            return False
        if not self.is_exist_column(table_name,view_cols):
            self.error += err
            return False 
        if len(select_cols) != len(words):
            self.error += f"{err}指定された列リストとワードリストの要素数が一致しません。\n"
            return False
        if and_or != "and" and and_or != "or":
            self.error += f"{err}組み合わせ演算子はandかorのみ有効です、{and_or}は無効な演算子です。\n"
            return False
        if match != "perfect" and match != "partial":
            self.error += f"{err}一致方式はpartialかperfectのみ有効です、{match}は無効な方式です。\n"
            return False

        sql_str = self.create_select_sql(table_name,select_cols,words,and_or,match,view_cols)
        if not sql_str:
            return False
        try:
            self.cursor.execute(sql_str)
            return self.cursor.fetchall()
        except Exception as ex:
            self.error += f"{err}{table_name}テーブルからのデータセレクトに失敗しました。\n"+str(ex)
            return False
    
    def select_by_id(self,table_name,id):
        err = "データセレクトエラー:<<select_by_id()\n"
        if not self.is_exist_table(table_name):
            self.error += f"{err}{table_name}というテーブルは存在しません。\n"
            return False
        
        try:
            num = int(id)
        except Exception as ex:
            self.error += f"{err}idは整数値を指定してください。\n"
            return False
        
        data = self.select(table_name,["id"],[id],"and","perfect")
        if data == False:
            self.error += err
            return False
        elif len(data) == 0:
            self.error += f"{err}id:'{id}'のデータは見つかりませんでした。\n"
            return False
        else:
            return data[0]
