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
    Equals "SELECT COUNT(*) FROM table WHERE condition".
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
