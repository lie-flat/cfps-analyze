import mysql.connector


USER = "cfps"
SERVER = "localhost"
PASSWORD = "cfpsMySQL111++"

db = mysql.connector.connect(
    host=SERVER,
    user=USER,
    password=PASSWORD
)
cursor = db.cursor()

cursor.execute('USE cfps')

def get(mysql_command : str) -> list:
    '''
    Return a query by a mysql command
    Format: [(x11,x12,x13),(x21,x22,x23),(x31,x32,x33)]
    Every tuple is a row, and all the component of a tuple is a column
    '''
    cursor.execute(mysql_command)
    return cursor.fetchall()

def count(mysql_table : str, condition : str = '') -> int:
    '''
    Return the row number of a table under a condition.
    Equals "SELECT COUNT(*) FROM {table} WHERE {condition}".
    '''
    if condition:
        cursor.execute(
            'SELECT COUNT(*) FROM ' +
            mysql_table + ' WHERE ' + 
            condition
        )
    else:
        cursor.execute('SELECT COUNT(*) FROM ' +mysql_table)
    return cursor.fetchall()[0][0]

def multicount(mysql_table_prefix: str,
               years: list = [2010, 2012, 2014, 2016, 2018],
               conditions: str or list = None,
               condition_format: list = None):
    '''
    Count for muiti years.
    :param mysql_table_prefix: prefix without underline '_', e.g. 'famecon'.
    :param years: the years we want to pick out.
    :param conditions: a string or list that controls the condition.
        Allow to have a variable part "{year}" to format, e.g. urban{year}
    :param condition_format: use to replace the variable part of conditions.
        Use 1 to represent the default year list.
    '''
    if conditions is None:
        return [count(mysql_table_prefix + '_' + str(year)) 
        for year in years]
    
    elif type(conditions) == str:
        list_to_return = []
        for i, year in enumerate(years):
            if condition_format is None:
                condition = conditions
            elif condition_format == 1:
                condition = conditions.format(year=year)
            else:
                # replace the variable part of conditions
                # e.g. urban{year} -> urban{2010}
                condition = conditions.format(year=condition_format[i])
            list_to_return.append(count(
                mysql_table_prefix + '_' + str(year),
                condition))
        return list_to_return

    elif type(conditions) == list:
        return [count(mysql_table_prefix + '_' + str(year), condition)
        for year, condition in zip(years, conditions)]

def csum(mysql_table: str, column: str, condition: str=''):
    '''
    Get the sum of one column under a condition.
    '''
    if condition:
        mysql_command = 'SELECT SUM({column}) FROM {table} WHERE {cond}'.format(
            column=column, table=mysql_table, cond=condition
        )
    else:
        mysql_command = 'SELECT SUM({column}) FROM {table}'.format(
            column=column, table=mysql_table
        )

    # print(mysql_command)
    cursor.execute(mysql_command)
    return cursor.fetchall()[0][0]

def urban_cond(year_index: int, is_urban: bool=True) -> str:
    '''
    Return the condition which can add to find out urban
    :param year_index: 0,1,2,3,4 for 2010, 2012, 2014, 2016, 2018.
    :param is_urban: True for urban and False for rural.
    '''
    URBAN_COLUMNS = ['urban', 'urban12', 'urban14', 'urban16', 'urban18']
    # In "urban" column of the database, 0 for rural and 1 for urban.
    return ' ' + URBAN_COLUMNS[year_index] + '=1 ' if is_urban else ' ' + URBAN_COLUMNS[year_index] + '=0 '

def ratio(list_a: list, b: list or int or float, acc='.2f') -> list:
    '''
    Return the ratio of two items in lists.
    :param acc: The format of output, '.2f' to reserve 2 slot of numbers
    '''
    if type(b) == list:
        return [format(a/b_, acc) for a, b_ in zip(list_a, b)]
    elif type(b) == int or float:
        return [format(a/b, acc) for a in list_a]

def myprint(string) -> None:
    '''
    Use to print without quotation mark(').
    '''
    print(repr(string).replace("'", ""))

def average(source_data : list, number : int or list, to_int : bool = True):
    if type(number) == int:
        average_data = [value / number for value in source_data]
    elif type(number) == list:
        average_data = [value / num for value, num in zip(source_data, number)]
    else:
        raise TypeError('number请输入int或者list类型')
    return [int(value) for value in average_data] if to_int else average_data