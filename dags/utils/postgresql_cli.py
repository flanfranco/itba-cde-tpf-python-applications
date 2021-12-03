import pandas as pd
from sqlalchemy import create_engine


class PostgresqlClient:
    def __init__(self, user, password, host, port, db, schema=None):
        self.dialect = 'postgresql'
        self.user = user        
        self.password = password
        self.host = host   
        self.port = port             
        self.db = db
        self.schema = schema

        self._engine = None

    def _get_engine(self):
        db_uri = f'{self.dialect}://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}'
        if not self._engine:
            self._engine = create_engine(db_uri)
        return self._engine

    def _connect(self):
        return self._get_engine().connect()

    @staticmethod
    def _cursor_columns(cursor):
        if hasattr(cursor, 'keys'):
            return cursor.keys()
        else:
            return [c[0] for c in cursor.description]

    def execute(self, sql, connection=None):
        if connection is None:
            connection = self._connect()
        return connection.execute(sql)

    def insert_from_frame(self, df, table, if_exists='append', index=False, **kwargs):
        connection = self._connect()
        with connection:
            if self.schema is None:
                df.to_sql(table, connection, if_exists=if_exists, index=index, **kwargs)
            else:
                df.to_sql(table, connection, self.schema, if_exists=if_exists, index=index, **kwargs)

    def to_frame(self, *args, **kwargs):
        cursor = self.execute(*args, **kwargs)
        if not cursor:
            return
        data = cursor.fetchall()
        if data:
            df = pd.DataFrame(data, columns=self._cursor_columns(cursor))
        else:
            df = pd.DataFrame()
        return df



