import psycopg2 



class pgConfig:
    def __init__(self, host, dbname, username, password) -> None:
        self.host = host
        self.dbname = dbname
        self.username = username
        self.password = password


class pgUtils:
    def __init__(self,config: pgConfig) -> None:
        self.config = config
        self.connection = self.create_conn()
        self.cur = self.connection.cursor() #一個object只能用一個conn跟cur

    def get_connect_string(self):
        return f"host={self.config.host} dbname={self.config.dbname} user={self.config.username} password={self.config.password}"
    
    def create_conn(self):
        try:
            conn = psycopg2.connect(self.get_connect_string())
            print(conn)
            return conn
        except Exception as e:
            print(f"Connection Erorr: {e}")

    def execute_sql_file(self, filepath: str):
        with open(filepath, 'r') as file:
            sql = file.read()
        self.cur.execute(sql)
        self.connection.commit()

    def execute_query(self,sql):
        self.cur.execute(f"""
            {sql}
        """)
        result = self.cur.fetchall()
        return result
    
    def execute_insert(self,sql):
        try:
            self.cur.execute(f"""
                {sql}
            """)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
        
    def insert_record(self, table: str, record: dict):
        record = self.flatten_record(record)
        columns = ', '.join(record.keys())
        values = ', '.join(['%s'] * len(record))
        sql = f"INSERT INTO {table} ({columns}) VALUES ({values})"

        values_list = list(record.values())
        
        self.cur.execute(sql, values_list)
        self.connection.commit()

    def flatten_record(self, record: dict, parent_key: str = '', sep: str = '_'):
        items = []
        for key, value in record.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, dict):
                items.extend(self.flatten_record(value, new_key, sep=sep).items())
            else:
                items.append((new_key, value))
        return dict(items)


    def adapt_value(self, value):
        if isinstance(value, dict):
            return str(value)  # Convert dict to string or handle appropriately
        return value

    def close(self):
        if self.cur:
            self.cur.close()
        if self.connection:
            self.connection.close()
