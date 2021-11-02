import sys
from utils import require_python_310
from cfps_shell import cfps
import mysql.connector
from functools import reduce
import numpy as np
import pandas as pd
from tqdm import tqdm


USER = "cfps"
SERVER = "localhost"
PASSWORD = "cfpsMySQL111++"


db = mysql.connector.connect(
    host=SERVER,
    user=USER,
    password=PASSWORD
)
cursor = db.cursor()


def write_to_db(start=2010, end=2018, dry=False):
    for year in cfps:
        if start <= year <= end:
            for table_base_name in cfps[year]:
                write_one_to_db(year, table_base_name, dry)


def write_one_to_db(year, table_base_name, dry=False):
    obj = cfps[year][table_base_name]
    dat = obj.data
    dat = dat.astype(generate_type_conversion_dict(obj.schema))
    table_name = f"{table_base_name}_{year}"
    print(f"Creating {table_name}...")
    if not dry:
        create_table_for_schema_if_not_exists(
            obj, table_name)
    order_tuple = generate_insert_order_tuple(obj.schema)
    dat = dat[order_tuple]
    insert_into_table(dat, obj.schema, table_name, dry)


def preprocess_value(v):
    return [None if pd.isna(x) else x for x in v]


def strip_columns(code, df):
    if code in df:
        df[code] = df[code].apply(str.strip)


def insert_into_table(dat, schema, table, dry=False):
    for column in schema:
        if schema[column]["type"] == "object":
            strip_columns(column, dat)
    vs = dat.values.tolist()
    for v in tqdm(vs):
        pv = preprocess_value(v)
        if not dry:
            cursor.execute(
                f"insert into {table} values ({('%s,'*dat.shape[1])[:-1]})", pv)


def init_db():
    cursor.execute("create database cfps")


def generate_type_conversion_dict(schema):
    return {
        s: "Int64" for s in schema if schema[s]["type"] == "enum" or schema[s]["type"] == "int32"
    }


def generate_insert_order_tuple(schema):
    return list(sorted(schema.keys()))


def sprintf_tuple_without_quotes(t):
    return '(' + ', '.join(t) + ')'


def create_table_for_schema_if_not_exists(info_obj, table_name):
    cursor.execute("use cfps;")
    sql = schema_to_table_sql(info_obj.schema, info_obj.primary, table_name)
    cursor.execute(sql)


def schema_to_table_sql(schema, primary, table_name):
    return 'CREATE TABLE IF NOT EXISTS ' + table_name + '(' + \
        ','.join(
            (k + ' ' + type_to_mysql_type(v) +
                ('' if k != primary else ' PRIMARY KEY')
             for k, v in sorted(schema.items()))
        ) +\
        (f' ,PRIMARY KEY {sprintf_tuple_without_quotes(primary)}' if isinstance(primary, tuple) else '') +\
        ') ENGINE=MYISAM DEFAULT CHARSET=utf8;'


def get_integer_type_for_max_value(m):
    if m <= 127:
        return "TINYINT(1)"
    elif m <= 32767:
        return "SMALLINT"
    elif m <= 8388607:
        return "MEDIUMINT"
    elif m <= 2147483647:
        return "INT"
    elif m <= 9223372036854775807:
        return "BIGINT"
    else:
        raise ValueError("Invalid max value.")


def get_integer_type_for_range(mm, r):
    mm = [m for m in mm if not pd.isna(m)]
    mm.append(0)  # Avoid empty sequence
    if isinstance(r, dict):
        if "max" in r and "min" in r:
            return get_integer_type_for_max_value(max(map(abs, mm)))
        else:
            return get_integer_type_for_max_value(max(max(map(abs, mm)), max(map(lambda x: abs(int(x)), r.keys()))))
    else:
        return get_integer_type_for_max_value(max(map(abs, mm)))


def type_to_mysql_type(s):
    match s:
        case {"type": 'int32', "minmax": mm, "range": r}:
            return get_integer_type_for_range(mm, r)
        case {"type": 'float64'}:
            return 'FLOAT'
        case {"type": 'enum', "minmax": mm, "range": r}:
            return get_integer_type_for_range(mm, r)
        case {"type": 'int8'}:
            return 'TINYINT(1)'
        case {"type": 'int16'}:
            return 'SMALLINT'
        case {"type": 'int64'}:
            return 'BIGINT'
        case {"type": 'object'}:
            return 'VARCHAR(10)'
        case _:
            raise ValueError("Invalid type in schema")


if __name__ == "__main__":
    require_python_310()
    if len(sys.argv) < 3:
        print("参数不足！")
        exit(1)
    match sys.argv[1:]:
        case ["db", "init"]:
            init_db()
        case ["db", "write", start, end]:
            write_to_db(int(start), int(end))
        case ["db", "write", year]:
            write_to_db(int(year))
        case ["db", "write"]:
            write_to_db()
        case ["db", "write-one", year, table_base_name]:
            write_one_to_db(int(year), table_base_name)
        case ["db", "write-one"]:
            print("参数不足！")
        case ["dry", "write", start, end]:
            write_to_db(int(start), int(end), dry=True)
        case ["dry", "write", start]:
            write_to_db(int(start), dry=True)
        case ["dry", "write"]:
            write_to_db(dry=True)
        case ["dry", "write-one", year, table_base_name]:
            write_one_to_db(int(year), table_base_name, dry=True)
        case ["dry", "write-one"]:
            print("参数不足！")
        case _:
            print("参数错误！")
            exit(1)
