    def clone_db(self,src_db,src_info_path,dest_db):
        err = "データベースクローンエラー:<<DB.clone_db()\n"
        if not os.path.exists(src_db):
            self.error += err+f"データベース:{src_db}は存在しませんでした。\n"
            return False
        if not os.path.exists(src_info_path):
            self.error += err+f"設定情報ファイル:{src_info_path}は存在しませんでした。\n"
            return False
        
        src_db_conn = sqlite.connection(src_db)
        src_db_cursor = src_db_conn.cursor()
        
        try:
            jsn = open(src_info_path,"r")
            src_info = json.load(jsn)
            src_create_sql_strs = [src_info[tbl]["create_sql"] for tbl in src_info["tables"]]
        except Exception as ex:
            self.error += err+f"クローン元の設定情報の取得に失敗しました。\n{str(ex)}\n"
            return False
        
        try:            
            for i,tbl in enumerate(src_info["tables"]):
                self.cursor.execute(src_create_sql_strs[i])
        except Exception as ex:
            self.error += err+f"データベースのテーブルクローンに失敗しました。\n{str(ex)}\n"
            return False
        
        self.conn.commit()
        self.info = src_info
        if self.save_setting():
            return True
        else:
            self.error += err
            return False
