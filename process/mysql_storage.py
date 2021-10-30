import sys
from utils import require_python_310
from cfps_shell import cfps
import mysql.connector

db = mysql.connector.connect(
  host="localhost",
  user="cfps",
  password="cfpsMySQL111++"
)
cursor = db.cursor()


def write_to_db_1nf():
    for year in cfps:
        for table_base_name in cfps[year]:
            obj = cfps[year][table_base_name]
            dat = obj.data
            schema = obj.schema

def init_db_1nf():
    cursor.execute("create database cfps1nf")

def type_to_mysql_type(s):
    match s:
        case 'int32':
            return 'INT'
        case 'float64':
            return 'FLOAT'
        case 'enum':
            return 'MEDIUMINT'
        case 'int8':
            return 'TINYINT'
        case 'int16':
            return 'SMALLINT'
        case 'int64':
            return 'BIGINT'
        case 'object':
            return 'VARCHAR'

if __name__ == "__main__":
    require_python_310()
    if len(sys.argv) < 3:
        print("参数不足！")
        exit(1)
    match sys.argv[1],sys.argv[2]:
        case "1nf","init":
            init_db_1nf()
        case "1nf","write":
            print("功能尚未实现")
        case _:
            print("参数错误！")
            exit(1)
