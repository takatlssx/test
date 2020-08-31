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
        
