def create_relational_table(self):
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
