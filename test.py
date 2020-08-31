    def clone_db(self,src_db,src_info,dest_db):
        err = "データベースクローンエラー:<<DB.clone_db()\n"
        if not os.path.exists(src_db):
            self.error += err+f"{src_db}は存在しませんでした。\n"
            return False
        
