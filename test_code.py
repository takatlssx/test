def get_data(self,table_name):
    data = []
    with open("","r",encoding="utf-8") as f:
        is_start = False
        for row in f:            
            if "<-@" == row:
                is_start = False
            if is_start:
                data.append(row.split(","))
            if "@->" in row:
                if row.replace("@->","") == table_name:
                    is_start = True
    retrun data
