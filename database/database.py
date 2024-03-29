
import sqlite3

class Database:
    def __init__(self, path, root=False):
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row

        self.cursor = self.connection.cursor()
        self.root = root

    def __del__(self):
        self.cursor.close()
        self.connection.close()

    def clear_all_tables(self) -> None:
        # confirm to really delete all data from all tables (cannot be restored)
        # ONLY WHEN NOT IN root mode which means self.root == False
        if not self.root:
            confirm = True if input(f"CAUTION: Clear all data from ALL tables? (y/n): ").lower() == "y" else False
            if not confirm:
                print("delete aborted.")
                return
        sql_fetch_tables = "SELECT name FROM sqlite_master WHERE type ='table';"
        self.cursor.execute(sql_fetch_tables)
        tables = [table[0] for table in self.cursor.fetchall()]

        print("deleted tables:", end="")
        for table in tables:
            self.cursor.execute(f"DELETE FROM {table}")
            self.connection.commit()
            print(f"{table}", end=", ")

    def delete_all_rows(self, table) -> None:
        # confirm to really delete all rows (cannot be restored)
        # ONLY WHEN NOT IN root mode which means self.root == False
        if not self.root:
            confirm = True if input(f"CAUTION: Delete all rows from '{table}'? (y/n): ").lower() == "y" else False
            if not confirm:
                print("delete aborted.")
                return

        # proceed to delete all rows
        sql = f"DELETE FROM {table};"
        self.cursor.execute(sql)
        self.connection.commit()
        print(f"deleted all rows from {table}")

    def ensure_column(self, table_name: str, column_name: str, data_type: str) -> bool:
        """
        Insert column into specified table, if it doesn't exist already.
        @param table_name: Name of the table in the database as string
        @param column_name: name of the column (case sensitive) to be looked for
        @param data_type: data type of the column if it needs to be created
                          ('TEXT'|'INTEGER'|'REAL'|'BLOB'|'NULL')

        @return: returns wether it existed already before
        """

        existing_columns: list = self.get_all(f'PRAGMA table_info({table_name});')
        col_exists: bool = any( map(lambda col: col['name'] == column_name, existing_columns) )

        if not col_exists:
            self.cursor.execute(f'ALTER TABLE {table_name} ADD {column_name} {data_type};')

        return col_exists

    def stringify(self, text, preserve_int=False):
        """
        preserve int is an optional argument to dump any value in stringify
        and no cases need to be considered beforehand
        ... so int values dont get put in quotes
        """

        # clean non-strings
        if type(text) != str:
            if preserve_int == True:
                return str(text)
            else:
                try:
                    return str(text)
                except:
                    print(text, "cannot be converted to string.")
                    return
        # clean strings
        text = text.replace("'", "''") # excape single quotes in text
        text = f"'{text}'"             # add single quotes to mark string
        return text

    def insert_row(self, table, row: dict) -> None:
        '''insert row in table if not exists. row provided as dict with {"col1" : val1, "col2" : val2}'''
        # sql command string
        sql = f"""
        insert into {table} ({','.join([column for column in row])})
        values ( {','.join([self.stringify(value, preserve_int=True) for value in row.values()])} );
        """
        # exceute command
        try:
            self.cursor.execute(sql)
            self.connection.commit()
        except sqlite3.IntegrityError:
            # print("row or primary key already exist")
            pass
        except sqlite3.OperationalError as e:
            print("[Operational Error]", e)
            quit()
        except Exception as e:
            print("ERROR:", e)

    def update_cell(self, table, column, new_value, primary_keys: dict = None, where: str = None) -> None:
        '''
        update at specific cell,
        specified by column and (multiple) primary keys
        @param table_name: Name of the table in the database as string
        @param column_name: name of the column (case sensitive) to be looked for
        @param new_value: new value of type string or int
        @param primary_keys: horizontal way of specifying wich cell is meant. As a dict like
                                {'ID': some_id }
        @param where: plain SQL input for more complex where clauses -> no primary_keys need to be specified

        '''

        if primary_keys is None and where is None:
            raise TypeError("either 'primary_keys' or 'where' needs to specify which cell to update.")

        if primary_keys:
            # specify rows by primary key(s). (where condition)
            condition = ' and '.join([
                f'{row} = {self.stringify(value)}'
                for row, value in primary_keys.items()
            ])
        else:
            if where:
                TypeError("'primary_keys' and 'where' cannot both have a value.")

            condition = where

        sql = f"""
        update {table}
        set {column} = {self.stringify(new_value, preserve_int=True)}
        where {condition};
        """
        try:
            self.cursor.execute(sql)
            self.connection.commit()
        except Exception as e:
            print("ERROR:", e)

    def get_all(self, sql_query) -> dict:
        """
        fetch all rows of sql query
        :param sql_query: string that contains query
        :return: dict with rows
        """
        self.cursor.execute(sql_query)
        json = [ dict(row) for row in self.cursor.fetchall() ]
        return json

    def insert_many(self, table: str, rows: list[dict]) -> None:
        '''
        Inserts a list of rows into db at once.
        Each row hast to be a dict like { 'col1': 'val1', col2: 2 }
        This is significantly faster that multiple insert_row() calls.
        '''
        if len(rows) == 0:
                return

        col_names = [col for col in rows[0]]

        # adds names of the columns (col1, col2, col3)
        sql = f"""INSERT INTO {table}"""
        sql += f"\n ({','.join(col_names)})"
        sql += "\nVALUES\n"

        # add rows of values (val1, val2, val3 ),\n (val1, val2, val3 )
        for i, row in enumerate(rows):
            sql += '('
            sql += ','.join([
                f"'{row[col]}'"
                if isinstance(row[col], str)
                else str(row[col])
                for col in col_names
            ])

            sql += ')'
            if i < len(rows) - 1:
                sql += ',\n'

        # exceute command
        try:
            self.cursor.execute(sql)
            self.connection.commit()
        except sqlite3.IntegrityError:
            # print("row or primary key already exist")
            pass
        except sqlite3.OperationalError as e:
            print("[Operational Error]", e)
            quit()
        except Exception as e:
            print("ERROR:", e)

    def execute(self, sql: str) -> None:
        ''' execute any command through plain SQL '''
        try:
            self.cursor.execute(sql)
            self.connection.commit()
        except Exception as e:
            print("ERROR:", e)

if __name__ == "__main__":
    pass
