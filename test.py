    def validate(self,table_name,data):
        #テーブル名チェック
        if not self.is_exist_table(table_name):
            self.error += "データ検証エラー：<<DB.validate()\n"
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
                        data[i] = int(data[i])
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
