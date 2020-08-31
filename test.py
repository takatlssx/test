    def copy_db(self,src_db,src_table_name,dest_table_name):
        if not os.path.exists(src_db):
            self.error f+= ""
            return False
        try:
            src_conn = sqlite.connection(src_db)
            src_cursor = src_conn.cursor()
            src_cursor.execute("select * from " + src_table_name)
            data = src_cursor.fetchall()
        
