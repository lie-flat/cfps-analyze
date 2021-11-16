import json
import sys
import os
from functools import reduce
from itertools import combinations
from cfps_shell import cfps
from utils import read_json, write_json


def data_path(year):
    return f"dataset/CFPS {year}/Data/Stata/"


def get_title_from_schema_filename(file):
    return file.replace('.dta.schemas.json', '')


def analyze_year(year):
    path = data_path(year)
    schemas = {}
    for file in os.listdir(path):
        fp = os.path.join(path, file)
        if os.path.isfile(fp) and fp.endswith(".dta.schemas.json"):
            schemas[file] = {"schema": read_json(fp)}
            schemas[file]["fields"] = set(schemas[file]["schema"].keys())
    common_fields = reduce(
        set.intersection, (s["fields"] for s in schemas.values()))

    def analyze_combination(ncom):
        r = {}
        for ss in combinations(schemas.keys(), ncom):
            cmmn = reduce(set.intersection, (schemas[x]["fields"] for x in ss)) \
                .difference(common_fields)
            r[reduce(lambda x, y: f"{x}|{y}", map(
                get_title_from_schema_filename, ss))] = list(cmmn)
        return r

    write_json({
        "common": list(common_fields),
        "n2": analyze_combination(2),
        "n3": analyze_combination(3),
        "n4": analyze_combination(4),
        "n5": analyze_combination(5),
    }, f"dataset/CFPS {year}/common.schemas.json")


def analyze_cross_year():
    adult = {x: cfps[x]["adult"].schema for x in range(2010, 2017, 2)}
    adult[2018] = cfps[2018].person.schema
    child = {x: cfps[x]["child"].schema for x in range(2010, 2017, 2)}
    child[2018] = cfps[2018].person.schema
    comm = {2010: cfps[2010].comm.schema, 2014: cfps[2014].comm.schema}
    famecon = {x: cfps[x]["famecon"].schema for x in range(2010, 2019, 2)}
    famconf = {x: cfps[x]["famconf"].schema for x in range(2010, 2019, 2)}

    def analyze_combination(schemas, ncom):
        r = {}
        for ss in combinations(schemas.keys(), ncom):
            cmmn = reduce(set.intersection, (set(schemas[x].keys()) for x in ss))
            r[reduce(lambda x, y: f"{x}|{y}", ss)] = list(cmmn)
        return r

    for var in ("adult", "child", "comm", "famecon", "famconf"):
        value = locals()[var]
        write_json({x: analyze_combination(value, x) for x in range(2, len(value) + 1)}, f"docs/{var}.json")


if __name__ == "__main__":
    version = sys.version_info
    if version.major != 3 or version.minor < 10:
        print(
            f"错误：不兼容的 Python 版本： {version.major}.{version.minor}\n请使用 Python 3.10 或更高版本来运行此脚本")
        sys.exit(1)
    if len(sys.argv) < 2:
        print("错误！参数不足。")
        sys.exit(1)
    match sys.argv[1]:
        case "year":
            if len(sys.argv) < 3:
                print("错误！参数不足。")
                sys.exit(1)
            year = int(sys.argv[2])
            analyze_year(year)
            print("OK")
        case "years":
            for year in 2010, 2011, 2012, 2014, 2016, 2018:
                analyze_year(year)
            print("OK")
        case "cross-year":
            analyze_cross_year()
            print("OK")
        case _:
            print("指令不存在")
