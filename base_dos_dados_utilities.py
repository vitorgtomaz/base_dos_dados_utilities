import basedosdados as bd
import inspect
import os
import pandas as pd

#%% As a class

class exploration_kit():
    def __init__(self, tables_and_addresses, project_id, columns_to_unpack = {}):
        self.error_log_ID = 0
        self.project_id = project_id

        self.df = pd.DataFrame(
            [[key, value, None, None, None, None] for (key, value) in tables_and_addresses.items()],
            columns=["Table", "Address", "SQL", "Column", "attributeColumns", "Attributes"])\
            .set_index("Table")
        pass
    
    def update_columns(self, all_or_specific = 'all'):
        ''''Missing Documentation''''
        if all_or_specific == 'all':
            self.df["Column"] = self.df["Address"].apply(lambda address: get_columns(address, self.project_id))
        else:
            self.df.loc[all_or_specific, ""] = self.df["Address"].apply(lambda address: get_columns(address, self.project_id))
        pass
    
    def set_attribute_columns(self, table: str, columns: list):
        ''''Missing Documentation''''
        self.df.loc[table]['attributeColumns'] = columns
        pass
    
    def unpack_table(self, all_or_specific = 'all', limit = 10000):
        '''Saves lists of unique values for each attribute column in each table'''
        if all_or_specific == 'all':
            self.df["Attributes"] = self.df.apply(
                get_unique_values(lambda row: get_unique_values(row.Address, row.attributeColumns, self.project_id, limit)))
        else:
            row = self.df.loc[all_or_specific]
            self.df.loc[all_or_specific].Attributes = get_unique_values(row.Address, row.attributeColumns, self.project_id, limit)
        pass
        
    def summary(self):
        '''Prints a summary of the object'''
        pass
    
    def list_attributes(self, table, attribute):
        ''''Missing Documentation''''
        return self.df.loc[table].Attributes.loc[attribute].values.tolist()
        pass
    
    def list_columns(self, table):
        ''''Missing Documentation''''
        return self.df.loc[table].Column.values.tolist()
        pass
    
    def survey_attributes_column(self, all_or_specific = 'all'):
        ''''Missing Documentation''''
        print('Use "Y/N" to set each column as attribute or not (c to cancel):')
        columns_to_set_as_attribute = []
        if all_or_specific == "all":
            tables = self.df.index.values.tolist()
        else:
            tables = [all_or_specific]

        for table in tables:
            for col in self.df['Column']:
                for column in col:
                    print(f'{table}: {column} - Y/N (c to cancel)')
                    x = input()
                    if x == 'c':
                        break
                    elif x in ['Y', 'y']:
                        columns_to_set_as_attribute.append(column)
                if x == 'c':
                    break
        return columns_to_set_as_attribute

    pass

def get_columns(db_address: str, project_id: str) -> list:
    '''Returns a list with all the columns names in the table returned by the
    Google Cloud query of 1 row in db_address.'''
    try:
        columns = bd.read_sql(f'SELECT * FROM {db_address} LIMIT 1',billing_project_id=project_id
                  ).columns

    except ValueError:
        caller_function = inspect.stack()[1][3]
        print(f'Method get_columns failed on table {db_address} (caller function: {caller_function})')
        columns = [f'Error fetching {db_address}\'s\' columns']
    
    return columns

def get_unique_values(db_address: str, columns_list: list, project_id: str, limit =  10000) -> dict:
    '''Returns dict with list of the unique values in each column in the input columns:
    {column: [unique values in column]}'''
    uv = []
    for col in columns_list:
        try:
            uv.append(
                [col, bd.read_sql(
                    f'SELECT DISTINCT {col} FROM {db_address} LIMIT {limit}',
                    billing_project_id=project_id
                    )[col].values.tolist()]
                )
    
        except ValueError:
            caller_function = inspect.stack()[1][3]
            print(f'Method get_unique_values failed to fetch column {col} \
                  unique values from table {db_address} (caller function: {caller_function})')
            uv.append([col, [f'Error fetching column {col}\'s\' unique values (table {db_address})']])
        
    return pd.DataFrame(uv, columns = ['Columns', 'Attributes']).set_index('Columns')
