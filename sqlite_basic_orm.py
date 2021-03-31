#!/usr/bin/env python
'''Basic sqlite3 ORM for python3.8+.

Gives ability to operate sqlite database as object or context manager object with list-input based methods.
Compatible with IPython scripts. Limited support for pandas.
Only simpliest SQL statements. There's no data checking and protection against SQL injection as well.
Limited support for tables, where names and column names have space (' ') and aphostropes ('). Not tolerated to input errors.
'''

# %%
import sqlite3

__author__ = 'Dmitriy Sidov'
__version__ = '0.6'
__maintainer__ = 'Dmitriy Sidov'
__email__ = 'dmitriy.sidov@gmail.com'
__status__ = 'Methods work fine with root db'

# %%
class SQLiteDB(object):

    def __assign_table(self, table_name):
        return SQLiteDB.Table(self, table_name)


    def __add_table(self, table_name):
        setattr(self, table_name, self.__assign_table(table_name))


    class Table(object):
        
        def _make_dict(self):
            result = dict()
            for i in range(len(self.table_info)):
                result[self.info[i][1]] = list()
                for j in range(len(self.data)):
                    result[self.table_info[i][1]].append(self.data[j][i])
            return result


        def __init__(self, SQLiteDB_obj, name):
            self._SQLiteDB_obj = SQLiteDB_obj
            self._name = name
            self.table_info = self.__SQLiteDB_obj.sqlite_table_info(table_name = self._name)
            self.data = self.__SQLiteDB_obj.sqlite_select(table_name = self._name)
            self.data_dict = self._make_dict()
            

        def insert(self):
            pass
            # print(self._SQLiteDB_obj.select(table_name = name))


    @staticmethod
    def __tuple_to_list(input_tuple):
        # Convert tuples to list so you can change them
        if input_tuple is None:
            return None
        else:
            result_list = list()
            for tuples in input_tuple:
                result_list.append(list(tuples))                   
            return result_list


    @staticmethod
    def __prettify_output(input_list):
    # Convert tuples to list and val-in-tuple-in-list to val-in-list : [('val1',), ('val2',)] -> ['val1','val2']    
        if input_list is None:
            return None
        elif len(input_list) == 1:
            return input_list[0]
        elif (len(input_list) > 1) and (len(input_list[0]) == 1):
            result_list = list()
            for lists in input_list:
                result_list.append(lists[0])
            return result_list
        else:
            return input_list


    def __init__(self, path):
        try:
            self.connection = sqlite3.connect(path)
            self.cursor = self.connection.cursor()
        except Exception as e:
            print(f'ERROR: {__name__}.__init__. {e}')
        # for columns in self.sqlite:

    def __del__(self):
        try:
            self.cursor.close()
            self.connection.commit()
            self.connection.close()
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.__del__. {e}')


    def sqlite_commit(self): 
        '''
        Сommit changes to the database.
        '''
        try:
            self.connection.commit()
        except Exception as e:
            print(f'ERROR: {__name__}.commit. {e}') 


    def sqlite_execute(self, statement, values=None, return_list=True, prettify_output=True, print_report=True, force_commit=True):
        '''
        Performs SQL statement to the database. Returns query results.
        
        Parameters
        ----------
        statement : str
        values : str, int, list (optional)
            Can be used for multiple rows insertion. If exists, using syntax (VALUES=?, val) instead (VALUES=val)
        prettify_output : boolean (default True)
            Changes output from val-in-tuple-in-list to val-in-list : [('val1',), ('val2',)] -> ['val1','val2']
        print_report : boolean
            Gives simple report if there were no errors. Prints statement if error occurs.
        force_commit : boolean (default True)
            Commit changes after statement execution.
        '''
        execute_many = False
        try:
            if values is None:
                self.cursor.execute(statement)
            else:
                if isinstance(values,list) or isinstance(values,tuple):
                    quest_marks = 0 # counting ? in statement
                    quest_set = {'?',',','(',')',' '}
                    for i in range(statement.find('VALUES')+6,len(statement)):
                        if statement[i] == '?':
                            quest_marks += 1
                        elif statement[i] not in quest_set:
                            break
                    if isinstance(values[0],list) or isinstance(values[0],tuple): 
                        execute_many = True
                    elif (quest_marks == 1) and (len(values) > 1):
                        if isinstance(values,tuple): # because you can't change tuple
                            values = list(values)
                        for i in range(0,len(values)):
                            if not (isinstance(values[i],list) or isinstance(values[i],tuple)):
                                values[i] = (values[i],)
                        execute_many = True
                else:
                    values = (values,)
                if execute_many:
                    self.cursor.executemany(statement, values)
                else:
                    self.cursor.execute(statement, values)
            result = self.cursor.fetchall()

        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.execute. {e}')
            if print_report:
                print(f'\nCheck statement:\n----------------\n{statement}')
            return None
        else:
            if force_commit:
                self.sqlite_commit()
            if return_list:
                result = self.__tuple_to_list(result)
            if prettify_output:
                result = self.__prettify_output(result)
            if print_report:
                print(f'SUCCESS: {__name__}.execute.')
                # print(f'\nCheck statement:\n----------------\n{statement}')
            return result


    # ----- db methods -----
    def sqlite_create_table(self, table_name, column_definitions, unique=None, primary_key=None, foreign_key=None, print_report=True, force_commit=True):
        '''
        Creates table in the database. 
       
        Parameters
        ----------
        table_name: str
        column_definitions: str, list of str
            Example: 'column_name1 data_type NOT NULL'
        unique: str or list
            Example: ['column_name1','column_name2']
        primary_key: str
        foreign_key: list / list of lists
            List structure: ['column_name1', 'reference_table', 'reference_column_name', 'additional actions']
        print_report: boolean 
            Gives simple report if there were no errors. Prints statement if error occurs.
        force_commit: boolean
            Commit changes after statement execution.
        '''
        if isinstance(column_definitions,list) or isinstance(column_definitions,tuple):
            if len(column_definitions) > 1:
                column_data_f = ',\n\t'.join(column_definitions)
            else: column_data_f = column_definitions[0]
        else:
            column_data_f = column_definitions
        statement = f'''CREATE TABLE {table_name} ({column_data_f}'''

        if unique is not None:
            if isinstance(unique,list) or isinstance(unique,tuple):
                statement += f''',\n\tUNIQUE ({', '.join(unique)})'''
            else:
                statement += f',\n\tUNIQUE ({unique})'

        if primary_key is not None:
            if isinstance(primary_key,list) or isinstance(primary_key,tuple):
                statement += f''',\n\tPRIMARY KEY ({', '.join(primary_key)})'''
            else:
                statement += f',\n\tPRIMARY KEY ({primary_key})'

        if foreign_key is not None:
            if (isinstance(foreign_key[0],list) or isinstance(foreign_key[0],tuple)) and (len(foreign_key) > 1):
                for keys in foreign_key:
                    statement += f''',\n\tFOREIGN KEY ({keys[0]}) \n\t\tREFERENCES {keys[1]} ({keys[2]})'''
                    if len(keys) > 3:
                        for i in range(3,len(keys)):
                            statement += f'\n\t\t{keys[i]}'
            else:
                statement += f''',\n\tFOREIGN KEY ({foreign_key[0]}) \n\t\tREFERENCES {foreign_key[1]} ({foreign_key[2]})'''
                if len(foreign_key) > 3:
                    for i in range(3,len(foreign_key)):
                        statement += f'\n\t\t{foreign_key[i]}'

        statement += '\n);'

        try:
            self.cursor.execute(statement)        
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.create_table. {e}')
            if print_report:
                print(f'\nCheck statement:\n----------------\n{statement}')
        else:
            if force_commit:
                self.sqlite_commit()
            if print_report:
                print(f'SUCCESS: {__name__}.create_table.')
                # print(f'\nCheck statement:\n----------------\n{statement}')            


    def sqlite_drop_table(self, table_name, disable_foreign_keys=False, print_report=True, force_commit=True):
        '''
        Removes a table from a table.

        Parameters
        ----------
        table_name : str, list
            One or several names of tables to delete.
        disable_foreign_keys : boolean (default False)
            Disable a foreign key constraint violations.
        print_report : boolean (default False)
            Gives simple report if there were no errors. Prints statement if error occurs.
        force_commit : boolean (default True)
            Commit changes after statement execution.  
        '''       
        no_errors = True
        execute_list = False
        if disable_foreign_keys:
            try:
                self.cursor.execute('PRAGMA foreign_keys = OFF;')
            except Exception as e:
                self.connection.rollback()
                print(f'ERROR: Disabling foreign_keys in {__name__}.drop_table. {e}. Check foreign_keys status.')
                no_errors = False
            else:
                if force_commit:
                    self.sqlite_commit()
        
        if no_errors:
            if isinstance(table_name, list) or isinstance(table_name, tuple): 
                execute_list = True
                statements = list()
                for name in table_name:
                    statements.append(f'''DROP TABLE {name};''')
            else: 
                statement = f'''DROP TABLE {table_name};'''
            
            try:
                if execute_list: 
                    for statement in statements:
                        self.cursor.execute(statement)
                else:
                    self.cursor.execute(statement)  
            except Exception as e:
                self.connection.rollback()
                print(f'ERROR: {__name__}.drop_table. {e}')
                no_errors = False
            else:
                if force_commit:
                    self.sqlite_commit() 
                if print_report:
                    print(f'SUCCESS: {table_name} was dropped.')
        if no_errors and disable_foreign_keys:
            try:
                self.cursor.execute('PRAGMA foreign_keys = ON;')
            except Exception as e:
                self.connection.rollback()
                print(f'ERROR: Enabling foreign_keys in {__name__}.drop_table. {e}. Check foreign_keys status.') 
            else:
                if force_commit:
                    self.sqlite_commit()   


    def sqlite_rename_table(self, old_name, new_name, print_report=True, force_commit=True):
        '''
        Renames table(s) in a database.

        Parameters
        ----------
        old_name : str, list
        new_name : str, list
            If list - names change according to the items order
        print_report : boolean (default False)
            Gives simple report if there were no errors. Prints statement if error occurs. Also count removed rows.
        force_commit : boolean (default True)
            Commit changes after statement execution.  
        '''    
        execute_list = False             
        if isinstance(old_name, list) or isinstance(old_name, tuple): 
            execute_list = True
            statements = list()
            for i in range(len(old_name)):
                statements.append(f'''ALTER TABLE {old_name[i]}\nRENAME TO {new_name[i]};''')
        else: 
            statement = f'''ALTER TABLE {old_name}\nRENAME TO {new_name};'''
        
        try:
            if execute_list:
                for statement in statements:
                    self.cursor.execute(statement)
            else:
                self.cursor.execute(statement)  
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.drop_table. {e}')
        else:
            if force_commit:
                self.sqlite_commit()
            if print_report:
                print(f'SUCCESS: Tables {old_name} was renamed at {new_name}')


    def sqlite_tables(self, prettify_output=True):
        '''
        Returns list of table names in database.

        Parameters
        ----------
        prettify_output : boolean (default True)
            Changes output from val-in-tuple-in-list to val-in-list : [('val1',), ('val2',)] -> ['val1','val2']
        '''   
        try:
            self.cursor.execute('''SELECT name FROM SQLITE_MASTER WHERE type='table';''')
            result = self.cursor.fetchall()
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.tables. {e}')
        if prettify_output:
            result = self.__prettify_output(result)
        return result


    def sqlite_table_info(self, table_name, prettify_output=True):
        '''
        Returns information about table colunms in list (list of list). 
        
        Items have structure: 
        list[0] : cid : int
        list[1] : name : text
        list[2] : data type : text
        list[3] : not null : boolean
        list[4] : default value : any format
        list[5] : primary key : boolean
        
        Parameters
        ----------
        table_name : str
        prettify_output : boolean (default True)
            Changes output from val-in-tuple-in-list to val-in-list : [('val1',), ('val2',)] -> ['val1','val2']
        '''
        try:
            self.cursor.execute(f'''SELECT * FROM pragma_table_info('{table_name}');''')
            result = self.cursor.fetchall()
        except Exception as e:
            self.connection.rollback()
            print(f'{__name__}.table ERROR. {e}')
        if prettify_output:
            result = self.__prettify_output(result)
        return result        


    def sqlite_schema(self, table_name, prettify_output=True):
        '''
        Returns the structure of а table in the database.

        Parameters
        ----------
        prettify_output : boolean (default True)
            Changes output from val-in-tuple-in-list to val-in-list : [('val1',), ('val2',)] -> ['val1','val2']
        '''  
        try:
            self.cursor.execute(f'''SELECT sql FROM SQLITE_MASTER WHERE type='table' and name = '{table_name}';''')
            result = self.cursor.fetchone()
        except Exception as e:
            self.connection.rollback()
            print(f'{__name__}.schema ERROR. {e}')
        if prettify_output:
            if result is not None:
                result = result[0]
        return result


    # ----- table methods -----
    def sqlite_add_column(self, table_name, column_definition, print_report=True, force_commit=True):
        '''
        Adds column(s) in a table.

        Parameters
        ----------
        table_name : str
        column_definitions: str, list
            Example: 'column_name1 data_type NOT NULL'
        print_report : boolean (default False)
            Gives simple report if there were no errors. Prints statement if error occurs. Also count removed rows.
        force_commit : boolean (default True)
            Commit changes after statement execution.  
        '''                  
        if isinstance(column_definition, list) or isinstance(column_definition, tuple): 
            statements = list()
            for definition in column_definition:
                statements.append(f'''ALTER TABLE {table_name}\nADD COLUMN {definition};''')
        else: 
            statement = f'''ALTER TABLE {table_name}\nADD COLUMN {column_definition};'''
        
        try:
            if isinstance(statements, list):
                for statement in statements:
                    self.cursor.execute(statement)
            else:
                self.cursor.execute(statement)  
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.add_column. {e}')
            no_errors = False
        else:
            if force_commit:
                self.sqlite_commit() 
            if print_report:
                print(f'''SUCCESS: Column(s) in {table_name} was added: {column_definition}''')


    def sqlite_rename_column(self, table_name, old_name, new_name, print_report=True, force_commit=True):
        '''
        Renames column(s) in a table.

        Parameters
        ----------
        table_name : str
        old_name : str, list
        new_name : str, list
            If list - names change according to the items order
        print_report : boolean (default False)
            Gives simple report if there were no errors. Prints statement if error occurs. Also count removed rows.
        force_commit : boolean (default True)
            Commit changes after statement execution.  
        '''                  
        if isinstance(old_name, list) or isinstance(old_name, tuple): 
            statements = list()
            for i in range(len(old_name)):
                statements.append(f'''ALTER TABLE {table_name}\nRENAME COLUMN {old_name[i]} TO {new_name[i]};''')
        else: 
            statement = f'''ALTER TABLE {table_name}\nRENAME COLUMN {old_name} TO {new_name};'''
        
        try:
            if isinstance(statements, list):
                for statement in statements:
                    self.cursor.execute(statement)
            else:
                self.cursor.execute(statement)  
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.rename_columns. {e}')
            no_errors = False
        else:
            if force_commit:
                self.sqlite_commit() 
            if print_report:
                print(f'SUCCESS: Columns in {table_name} was renamed: {old_name} at {new_name}')



    def sqlite_count_rows(self, table_name, search_condition=None, print_report=False):
        '''
        Returns number of items in a table.

        Parameters
        ----------
        table_name : str
        search_condition : str
            Full sqlite WHERE clause (without WHERE).
        print_report : Prints statement if error occurs.
        '''
        if search_condition is None: 
            statement = f'''SELECT COUNT(*)\nFROM {table_name};'''
        else:
            statement = f'''SELECT COUNT(*)\nFROM {table_name}\nWHERE {search_condition};'''
        try:
            self.cursor.execute(statement)
            result = self.cursor.fetchone()
        except Exception as e:
            print(f'ERROR: {__name__}.count_rows. {e}')
            if print_report:
                print(f'\nStatement:\n----------\n{statement}')
            return None
        else:
            return result[0]


    def sqlite_select(self, table_name, column_names='*', distinct=False, join=None, where=None, order_by=None, limit_offset=None, group_by=None, having=None, return_list=True, prettify_output=True, print_report=True, force_commit=True):
        '''
        Returns query data from a table. Doesn't fully support the SELECT statement.
        Available clauses are shown by arguments.

        Parameters
        ----------
        column_names : str or list
        table_name : str
        distinct : boolean
        join : 3 element list or list of it : [join type, table, full condition]
            Example: ['INNER','table1','table1.col1 = table2.col1']
        where : str
            Full sqlite WHERE clause.
        order_by : str (full condition). 
            Full sqlite ORDER BY clause. Example: column1 ASC/DESC
        limit_offset : str/int if limit only or list if offset included: [limit, offset] 
        group_by: str or list
            Example : [column1, column2, ...]
        having: str
            Full sqlite HAVING clause.
        print_report : boolean 
            Gives simple report if there were no errors. Prints statement if error occurs.
        force_commit : boolean (default True)
            Commit changes after statement execution.        
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
            statement += f'''\nWHERE {where}'''

        if order_by is not None:
            if (isinstance(order_by,list) or isinstance(order_by,tuple)):
                order_len = len(order_by)
                statement += f'''ORDER BY\n\t{order_by[0]}'''
                if order_len > 1:
                    for i in range(1,order_len):
                        statement += f''',\n\t{order_by[i]}'''
            else:
                statement += f'''\nORDER BY {order_by}'''

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
                statement += f'''\nGROUP BY {group_by}'''

        if having is not None:
            statement += f'''\nHAVING {having}'''

        statement += f';'

        try:
            self.cursor.execute(statement)
            result = self.cursor.fetchall()        
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.select. {e}')
            if print_report:
                print(f'\nCheck statement:\n----------------\n{statement}')
            return None
        else:
            if force_commit:
                self.sqlite_commit()
            if return_list:
                result = self.__tuple_to_list(result)
            if prettify_output:
                result = self.__prettify_output(result)
            if print_report:
                print(f'SUCCESS: {__name__}.select.')
                # print(f'\nCheck statement:\n----------------\n{statement}')
            return result


    def sqlite_insert(self, table_name, column_names, values, replace=False, print_report=True, force_commit=True):
        '''
        Insert new row(s) into a table.

        Parameters
        ----------
        table_name : str
        column_names: str, list
            Use '*' to insert values into all existing tables in the appropriate order.
        values : str, int, list, list of lists
        replace : boolean
            Ignore UNIQUE or PRIMARY KEY constraint violation. If True, it deletes existing row and inserting new one.
        print_report : boolean 
            Gives simple report if there were no errors. Prints statement if error occurs. Also count planned / resulting insertions.
        force_commit : boolean (default True)
            Commit changes after statement execution.  
        '''
        if column_names == '*':
            table_info = self.sqlite_table_info(table_name,prettify_output=False)
            column_names = list()
            for columns in table_info:
                column_names.append(columns[1])
        
        execute_many = False

        if replace:
            ins_or_repl = 'INSERT OR REPLACE INTO'
        else:
            ins_or_repl = 'INSERT INTO'

        if isinstance(column_names,list) or isinstance(column_names,tuple):
            if len(column_names) > 1:
                statement = f'''{ins_or_repl} {table_name} ({', '.join(column_names)})\nVALUES(?{',?'*(len(column_names)-1)})'''
                if isinstance(values[0],list) or isinstance(values[0],tuple):
                    execute_many = True
            else: 
                statement = f'''{ins_or_repl} {table_name} ({column_names[0]})\nVALUES(?)'''
                if isinstance(values,list) or isinstance(values,tuple):
                    execute_many = True
                    if isinstance(values,tuple): # because you can't change tuple
                        values = list(values)
                    for i in range(0,len(values)):
                        if not (isinstance(values[i],list) or isinstance(values[i],tuple)):
                            values[i] = (values[i],)
                else:
                    values = (values,)
        else:
            statement = f'''{ins_or_repl} {table_name} ({column_names})\nVALUES(?)'''
            if isinstance(values,list) or isinstance(values,tuple):
                execute_many = True
                if isinstance(values,tuple): # because you can't change tuple
                    values = list(values)
                for i in range(0,len(values)):
                    if not (isinstance(values[i],list) or isinstance(values[i],tuple)):
                        values[i] = (values[i],) 
            else:          
                values = (values,)
        
        if print_report:
            search_len = self.sqlite_count_rows(table_name)
            if not (isinstance(values,list) or isinstance(values,tuple)):
                values_len = 1
            elif (isinstance(values[0],list) or isinstance(values[0],tuple)) and (len(column_names) == len(values)):
                values_len = 1 # when 1 val list for col list
            else:
                values_len = len(values)

        try:
            if execute_many:
                self.cursor.executemany(statement, values)
            else:
                self.cursor.execute(statement, values)
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.insert. {e}')
            if print_report:
                print(f'\nCheck statement:\n----------------\n{statement}')
        else:
            if force_commit:
                self.sqlite_commit()
            if print_report:
                delta_len = self.sqlite_count_rows(table_name) - search_len
                print(f'SUCCESS: {__name__}.insert. Added rows: {delta_len}/{values_len}')
                # print(f'\nCheck statement:\n----------------\n{statement}')


    def sqlite_delete(self, table_name, search_condition, print_report=True, force_commit=True):
        '''
        Removes row(s) with search condition from a table.

        Parameters
        ----------
        table_name : str
        search_condition : str
            Full sqlite WHERE clause.
        print_report : boolean 
            Gives simple report if there were no errors. Prints statement if error occurs. Also count removed rows.
        force_commit : boolean (default True)
            Commit changes after statement execution.  
        '''
        statement = f'''DELETE FROM {table_name}\nWHERE {search_condition};'''
       
        if print_report:
            search_len = self.sqlite_count_rows(table_name, search_condition)
        try:
            self.cursor.execute(statement)
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.delete. {e}')
            if print_report:
                print(f'\nCheck statement:\n----------------\n{statement}')
        else:
            if force_commit:
                self.sqlite_commit()
            if print_report:
                print(f'SUCCESS: {__name__}.remove. Removed rows: {search_len}')
                # print(f'\nStatement:\n----------\n{statement}')


    def sqlite_update(self, table_name, column_names, values, search_condition, print_report=True, force_commit=True):
        '''
        Update existing data in a table.

        Parameters
        ----------
        table_name : str
        column_names : str, list
            Use '*' to update values in all existing tables in the appropriate order.
        values : str, list
        search_condition : str
            Full sqlite WHERE clause.
        print_report : boolean 
            Gives simple report if there were no errors. Also count updated rows.
        force_commit : boolean (default True)
            Commit changes after statement execution.  
        '''
        if column_names == '*':
            table_info = self.sqlite_table_info(table_name,prettify_output=False)
            column_names = list()
            for columns in table_info:
                column_names.append(columns[1])

        if print_report:
            search_len = self.sqlite_count_rows(table_name, search_condition)

        statement = f'''UPDATE {table_name}\nSET '''        
        
        if isinstance(column_names,list) or isinstance(column_names,tuple):
            columns_len = len(column_names)
            statement += f'''{column_names[0]} = ?'''
            if columns_len > 1:
                for i in range(1,columns_len):
                    statement += f''',\n\t{column_names[i]} = ?'''
        else:
            statement += f'''{column_names} = ?'''

        statement += f'''\nWHERE {search_condition};''' 
        
        if not (isinstance(values,list) or isinstance(values, tuple)):
            values = (values,)

        try:
            self.cursor.execute(statement,values)
        except Exception as e:
            self.connection.rollback()
            print(f'ERROR: {__name__}.update. {e}')
            if print_report:
                print(f'\nCheck statement:\n----------------\n{statement}')
        else:
            if force_commit:
                self.sqlite_commit()
            if print_report:
                print(f'SUCCESS: {__name__}.insert. Updated rows: {search_len}')       
                # print(f'\nCheck statement:\n----------------\n{statement}')


class SQLiteContextDB(SQLiteDB):

    def __enter__(self): # context function
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        self.cursor.close()
        if isinstance(exc_value, Exception):
            self.connection.rollback()
        else:
            self.connection.commit()
        self.connection.close()