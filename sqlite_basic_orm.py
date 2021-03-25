#!/usr/bin/env python
"""Basic sqlite3 ORM for python3.8+.

Gives ability to operate sqlite database as object or context manager object with list-input based methods.
Compatible with IPython scripts. Limited support for pandas.
Only simpliest SQL statements. There's no data checking and protection against SQL injection as well.
Doesn't support table names and column names with space (' ') and aphostropes (') in it.
"""

# %%
import sqlite3

__author__ = 'Dmitriy Sidov'
__version__ = '0.2'
__maintainer__ = 'Dmitriy Sidov'
__email__ = 'dmitriy.sidov@gmail.com'
__status__ = 'Bug hunt time!'

# %%
class OpenDB:

    @staticmethod
    def _convert_tuple(result_tuple):
        # Convert tuples to list and val-in-tuple-in-list to val-in-list : [('val1',), ('val2',)] -> ['val1','val2']
        if len(result_tuple) == 0:
            return None
        else:
            result_list = list()
            if len(result_tuple[0]) == 1:
                for tuples in result_tuple:
                    result_list.append(tuples[0])
            else:
                for tuples in result_tuple:
                    result_list.append(list(tuples))                   
            return result_list


    def __init__(self, path):
        try:
            self.connection = sqlite3.connect(path)
            self.cursor = self.connection.cursor()
        except Exception as e:
            print(f'ERROR: {__name__}.__init__. {e}')


    def __del__(self):
        try:
            self.cursor.close()
            self.connection.commit()
            self.connection.close()
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.__del__. {e}')


    def commit(self): # commit changes to save them
        try:
            self.connection.commit()
        except Exception as e:
            print(f'ERROR: {__name__}.commit. {e}') 


    def tables(self, prettify_return=True):
        """
        Parameter
        ----------
        :conert_tuple: change output from val-in-tuple-in-list to val-in-list : [('val1',), ('val2',)] -> ['val1','val2']
        """    
        try:
            self.cursor.execute('''SELECT name FROM SQLITE_MASTER WHERE type='table';''')
            result = self.cursor.fetchall()
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.tables. {e}')
        if prettify_return:
            result = self._convert_tuple(result)
        return result


    def schema(self, table_name, prettify_return=True):
        try:
            self.cursor.execute(f'''SELECT sql FROM SQLITE_MASTER WHERE type='table' and name = '{table_name}';''')
            result = self.cursor.fetchone()
        except Exception as e:
            self.connection.rollback()
            print(f'{__name__}.schema ERROR. {e}')
        if prettify_return:
            result = result[0]
        return result


    def execute(self, statement, values=None, prettify_return=True, print_report=True, force_commit=True):
        """
        if values = None - excute(x=a)
        if values is not None - execute(x=?, val)
        """
        try:
            if values is None:
                self.cursor.execute(statement)
            else:
                self.cursor.execute(statement, values)
            result = self.cursor.fetchall()
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.execute. {e}')
            print(f'\nStatement:\n----------\n{statement}')
            return None
        else:
            if print_report:
                print(f'SUCCESS: {__name__}.execute.')
                print(f'\nStatement:\n----------\n{statement}')
            if force_commit:
                self.commit()
            if prettify_return:
                result = self._convert_tuple(result)
            return result

    def execute_many(self, statement, values, prettify_return=True, print_report=True, force_commit=True):
        """
        statement: full SQL statement with ? on val places
        """
        try:
            self.cursor.executemany(statement, values)
            result = self.cursor.fetchall()
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.execute_many. {e}')
            print(f'\nStatement:\n----------\n{statement}')
            return None
        else:
            if print_report:
                print(f'SUCCESS: {__name__}.execute_many.')
                print(f'\nStatement:\n----------\n{statement}')
            if force_commit:
                self.commit()
            if prettify_return:
                result = self._convert_tuple(result)
            return result

    def count_rows(self, table_name, search_condition=None, prettify_return=True):
        if search_condition is None: 
            statement = f'''SELECT COUNT(*)\nFROM {table_name};'''
        else:
            statement = f'''SELECT COUNT(*)\nFROM {table_name}'''
            if isinstance(search_condition,list) or isinstance(search_condition,tuple):
                where_len = len(search_condition)
                statement += f'''\nWHERE {search_condition[0]}''' 
                
            else:
                statement += f'''\nWHERE {search_condition};''' 
        try:
            self.cursor.execute(statement)
            result = self.cursor.fetchone()
        except Exception as e:
            print(f'ERROR: {__name__}.count_rows. {e}')
            print(f'\nStatement:\n----------\n{statement}')
            return None
        else:
            if prettify_return:
                result = result[0]
            return result
          

    def create_table(self, table_name, column_clauses, unique=None, primary_key=None, foreign_key=None, print_report=True, force_commit=True):
        """
        Parameters
        ----------
        :table_name: str
        :column_clauses: str or list/tuple of str. Example: 'column_name1 data_type NOT NULL / DEFAULT 0'
        :unique: str or list/tuple. Example: ['column_name1','column_name2']
        :primary_key: str
        :foreign_key_lists: list / list of lists : ['column_name1', 'reference_table', 'reference_column_name', 'additional actions']
        :custom_line: str or list/tuple : add custom statement(s) to the end
        :print_report: boolean. Print report about sucess (or not)
        :force_commit: commit changes after statement execution
        """
        if isinstance(column_clauses,list) or isinstance(column_clauses,tuple):
            if len(column_clauses) > 1:
                column_data_f = ',\n\t'.join(column_clauses)
            else: column_data_f = column_clauses[0]
        else:
            column_data_f = column_clauses
        statement = f'''CREATE TABLE {table_name} ({column_data_f}'''

        if unique is not None:
            if isinstance(unique,list) or isinstance(unique,tuple):
                statement += f''',\n\tUNIQUE ({', '.join(unique)})'''
            else:
                statement += f',\n\tUNIQUE ({unique})'

        if primary_key is not None:
            statement += f',\n\tPRIMARY KEY ({primary_key})'

        if foreign_key is not None:
            if (isinstance(foreign_key[0],list) or isinstance(foreign_key[0],tuple)) and (len(foreign_key) > 1):
                for keys in foreign_key:
                    statement += f''',\n\tFOREIGN KEY ({keys[0]}) \n\tREFERENCES {keys[1]} ({keys[2]}) '''
                    if len(keys) > 3:
                        for i in range(3,len(keys)):
                            statement += f'\n\t\t{keys[i]}'
            else:
                statement += f''',\n\tFOREIGN KEY ({foreign_key[0]}) \n\tREFERENCES {foreign_key[1]} ({foreign_key[2]}) '''
                if len(foreign_key) > 3:
                    for i in range(3,len(foreign_key)):
                        statement += f'\n\t\t{foreign_key[i]}'

        statement += '\n);'

        try:
            self.cursor.execute(statement)        
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.create_table. {e}')
            print(f'\nStatement:\n----------\n{statement}')
        else:
            if print_report:
                print(f'SUCCESS: {__name__}.create_table.')
                print(f'\nStatement:\n----------\n{statement}')
            if force_commit:
                self.commit()


    def select(self, table_name, column_names, distinct=False, join=None, where=None, order_by=None, limit_offset=None, group_by=None, having=None, prettify_return=True, print_report=False, force_commit=True):
        '''
        Parameters
        ----------
        :column_names: str or list/tuple
        :table_name: str
        :distinct: boolean
        :join: 3 element list/tuple or list of it : [join type, table, full condition]. EX: ['INNER','table1','table1.col1 = table2.col1']
        :where: str (full condition). EX
        :order_by: str (full condition). EX: column1 ASC/DESC
        :limit_offset: str/int if limit only or list/tuple if offset included: [limit, offset] 
        :group_by: str or list/tuple : [column1, column2, ...]
        :having: full search condition
        :print_statement: boolean. Print your final statement
        '''
        if distinct:
            statement = 'SELECT DISTINCT '
        else:
            statement = 'SELECT '
        
        if isinstance(column_names,list) or isinstance(column_names,tuple):
            if len(column_names) > 1:
                column_names = ',\n\t'.join(column_names)
            else:
                column_names = column_names[0]
            statement += f'''{column_names}\nFROM {table_name}'''
        else:
            statement += f'''{column_names}\nFROM {table_name}'''

        if join is not None:
            if (isinstance(join[0],list) or isinstance(join[0],tuple)) and (len(join) > 1):
                for joins in join:
                    statement += f'''\n\t {joins[0]} JOIN {joins[1]} ON {joins[2]}'''
            else:
                    statement += f'''\n\t {join[0]} JOIN {join[1]} ON {join[2]}'''

        if where is not None:
            if (isinstance(where,list) or isinstance(where,tuple)):
                where_len = len(where)
                statement = f'''WHERE {where[0]}'''
                if where_len > 1:
                    for i in range(1,len(where)):
                        statement += f'''\n\tAND {where[i]}'''
            else:
                statement += f'''WHERE {where}'''

        if order_by is not None:
            if (isinstance(order_by,list) or isinstance(order_by,tuple)):
                order_len = len(order_by)
                statement += f'''ORDER BY\n\t{order_by[0]}'''
                if order_len > 1:
                    for i in range(1,order_len):
                        statement += f''',\n\t{order_by[i]}'''
            else:
                statement += f'''ORDER BY {order_by}'''

        if limit_offset is not None:
            if isinstance(limit_offset,list) or isinstance(limit_offset,tuple):
                limit_len = len(limit_offset)
                if limit_len > 1:
                    statement += f'''\nLIMIT {str(limit_offset[0])} OFFSET {str(limit_offset[1])}''' 
                else:
                    statement += f'''\nLIMIT {str(limit_offset[0])}'''       
            else:
                statement += f'''\nLIMIT {str(limit_offset)}'''
                
        if group_by is not None:
            if (isinstance(group_by,list) or isinstance(group_by,tuple)):
                group_len = len(group_by)
                statement += f'''\nGROUP BY\n\t{group_by[0]}'''
                if group_len > 1:
                    for i in range(1,group_len):
                        statement += f''',\n\t{group_by[i]}'''
            else:
                statement += f'''\nGROUP BY {order_by}'''

        if having is not None:
            statement += f'''\nHAVING {having}'''

        statement = f'\n;'

        try:
            self.cursor.execute(statement)
            result = self.cursor.fetchall()        
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.select. {e}')
            print(f'\nStatement:\n----------\n{statement}')
            return None
        else:
            if print_report:
                print(f'SUCCESS: {__name__}.select.')
                print(f'\nStatement:\n----------\n{statement}')
            if force_commit:
                self.commit()
            if prettify_return:
                result = self._convert_tuple(result)
            return result


    def insert(self, table_name, column_names, values, print_report=True, force_commit=True):
        """
        Parameters
        ----------
        :table_name: str
        :column_names: str or list/tuple
        :values: str, list/tuple, list of lists
        """
        execute_many = False
        if print_report:
            search_len = self.count_rows(prettify_return=True)
            values_len = len(values)
        if isinstance(column_names,list) or isinstance(column_names,tuple):
            if len(column_names) > 1:
                statement = f'''INSERT INTO {table_name} ({', '.join(column_names)})\nVALUES(?{',?'*(len(column_names)-1)})'''
                if isinstance(values[0],list) or isinstance(values[0],tuple):
                    execute_many = True
            else: 
                statement = f'''INSERT INTO {table_name} ({column_names})\nVALUES(?)'''
                if isinstance(values,list) or isinstance(values,tuple):
                    execute_many = True
        else:
            statement = f'''INSERT INTO {table_name} ({column_names})\nVALUES(?)'''
            if isinstance(values,str):
                values = (values,)
        try:
            if execute_many:
                self.cursor.executemany(statement, values)
            else:
                self.cursor.execute(statement, values)
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.insert. {e}')
            print(f'\nStatement:\n----------\n{statement}')
        else:
            if print_report:
                delta_len = self.count_rows(prettify_return=True) - search_len
                print(f'SUCCESS: {__name__}.insert. Added rows: {delta_len}/{values_len}')
                print(f'\nStatement:\n----------\n{statement}')
            if force_commit:
                self.commit()


    def update(self, table_name, column_names, values, search_condition, print_report=True, force_commit=True):
        '''
        Parameters
        ----------
        :table_name: str
        :column_names: str or list/tuple
        :values: str or list/tuple of str
        :search_condition: search condition clauses, str or list
        '''
        if print_report:
            search_len = self.count_rows(table_name, search_condition)

        statement = f'''UPDATE {table_name}\nSET '''        
        if isinstance(column_names,list) or isinstance(column_names,tuple):
            columns_len = len(column_names)
            statement += f'''{column_names[0]} = ?'''
            if columns_len > 1:
                for i in range(1,columns_len):
                    statement += f''',\n\t{column_names[i]} = ?'''
        else:
            statement += f'''{column_names} = ?'''

        if isinstance(search_condition,list) or isinstance(search_condition,tuple):
            where_len = len(search_condition)
            statement += f'''\nWHERE {search_condition}''' 
            if where_len > 1:
                for i in range(1,where_len):
                    statement += f''',\n\tAND {search_condition[i]}'''
            statement += ';'
        else:
            statement += f'''\nWHERE {search_condition};''' 
        
        try:
            self.cursor.execute(statement,values)
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.update. {e}')
            print(f'\nStatement:\n----------\n{statement}')
        else:
            if print_report:
                print(f'SUCCESS: {__name__}.insert. Updated rows: {search_len}')       
                print(f'\nStatement:\n----------\n{statement}')
            if force_commit:
                self.commit()


    def replace(self, table_name, column_names, values, print_report=True, force_commit=True):
        """
        Parameters
        ----------
        :table_name: str
        :column_names: str or list/tuple
        :values: str, list/tuple, list of lists
        """
        execute_many = False
        if print_report:
            search_len = self.count_rows(prettify_return=True)
            values_len = len(values)

        if isinstance(column_names,list) or isinstance(column_names,tuple):
            if len(column_names) > 1:
                statement = f'''INSERT OR REPLACE INTO {table_name} ({', '.join(column_names)})\nVALUES(?{',?'*(len(column_names)-1)})'''
                if isinstance(values[0],list) or isinstance(values[0],tuple):
                    execute_many = True
            else: 
                statement = f'''INSERT OR REPLACE INTO {table_name} ({column_names})\nVALUES(?)'''
                if isinstance(values,list) or isinstance(values,tuple):
                    execute_many = True
        else:
            statement = f'''INSERT OR REPLACE INTO {table_name} ({column_names})\nVALUES(?)'''
            if isinstance(values,str):
                values = (values,)
        try:
            if execute_many:
                self.cursor.executemany(statement, values)
            else:
                self.cursor.execute(statement, values)
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.replace. {e}')
            print(f'\nStatement:\n----------\n{statement}')
        else:
            if print_report:
                delta_len = self.count_rows(prettify_return=True) - search_len
                print(f'SUCCESS: {__name__}.replace. Added rows: {delta_len}/{values_len}')
                print(f'\nStatement:\n----------\n{statement}')
            if force_commit:
                self.commit()

    def delete(self, table_name, search_condition, print_report=True, force_commit=True):
        statement = f'''DELETE FROM {table_name}\nWHERE {search_condition};'''
        if isinstance(search_condition,list) or isinstance(search_condition,tuple):
            where_len = len(search_condition)
            statement += f'''\nWHERE {search_condition}''' 
            if where_len > 1:
                for i in range(1,where_len):
                    statement += f''',\n\tAND {search_condition[i]}'''
            statement += ';'
        else:
            statement += f'''\nWHERE {search_condition};''' 
       
        if print_report:
            search_len = self.count_rows(table_name, search_condition)
        try:
            self.cursor.execute(statement)
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.delete. {e}')
            print(f'\nStatement:\n----------\n{statement}')
        else:
            if print_report:
                delta_len = self.count_rows(prettify_return=True) - search_len
                print(f'SUCCESS: {__name__}.insert. Removed rows: {delta_len}')
                print(f'\nStatement:\n----------\n{statement}')
            if force_commit:
                self.commit()


    def drop_table(self, table_name, disable_foreign_keys=False, print_report=False, force_commit=True):
        no_errors = True
        if disable_foreign_keys:
            try:
                self.cursor.execute('PRAGMA foreign_keys = OFF;')
            except Exception as e:
                self.connection.rollback()
                print(f'ERROR: Disabling foreign_keys in {__name__}.drop_table. {e}. Check foreign_keys status.')
                no_errors = False
            else:
                if force_commit:
                    self.commit()
        
        if no_errors:
            statement = f'''DROP TABLE {table_name};'''

            try:
                self.cursor.execute(statement)
            except Exception as e:
                self.connection.rollback()
                print(f'ERROR: {__name__}.drop_table. {e}')
                no_errors = False
            else:
                if print_report:
                    print(f'SUCCESS: Removed {table_name}')
                if force_commit:
                    self.commit()       
           
        if no_errors and disable_foreign_keys:
            try:
                self.cursor.execute('PRAGMA foreign_keys = ON;')
            except Exception as e:
                self.connection.rollback()
                print(f'ERROR: Enabling foreign_keys in {__name__}.drop_table. {e}. Check foreign_keys status.') 
            else:
                if force_commit:
                    self.commit()   


    def alter_table(self, table_name, new_table_name, print_report=False, force_commit=True): 
        statement = f'''ALTER TABLE {table_name}\nRENAME TO {new_table_name};'''
        try:
            self.cursor.execute(statement)
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.alter_table. {e}')
            print(f'\nStatement:\n----------\n{statement}')
        else:
            if print_report:
                print(f'SUCCESS: Changed table name {table_name} to {new_table_name}')
                print(f'\nStatement:\n----------\n{statement}')
            if force_commit:
                self.commit()


    def rename_column(self, table_name, column_name, new_column_name, print_report=False, force_commit=True):
        statement = f'''ALTER TABLE {table_name}\nRENAME COLUMN {column_name} TO {new_column_name};'''
        try:
            self.cursor.execute(statement)
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.rename_column. {e}')
            print(f'\nStatement:\n----------\n{statement}')
        else:
            if print_report:
                print(f'SUCCESS: Changed column name {column_name} to {new_column_name}')
                print(f'\nStatement:\n----------\n{statement}')
            if force_commit:
                self.commit()

    
class ContextOpenDB(OpenDB):

    def __enter__(self): # context function
        return self


    def __exit__(self, ext_type, exc_value, traceback):
        self.cursor.close()
        if isinstance(exc_value, Exception):
            self.connection.rollback()
        else:
            self.connection.commit()
        self.connection.close()