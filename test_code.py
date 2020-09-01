    def import_csv(self,csv_path,table_name):
        err = "csvインポートエラー:<<import_csv()\n"
        if not os.path.exists(csv_path):
            self.error += f"{err}csvファイル:{csv_path}が見つかりませんでした。\n"
            return False
        
        if not self.is_exist_table(table_name):
            self.error += f"{err}{table_name}テーブルは存在しません。\n"
            return False

        data_2d = []
        try:
            with open(csv_path,"r",encoding="utf-8") as f:
                data_2d = [row.split(",") for row in f]
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
