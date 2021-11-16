import json
import sys
import os
from functools import reduce
from itertools import combinations


def read_json(fp):
    with open(fp, encoding="utf-8") as f:
        return json.load(f)


def write_json(obj, fp):
    with open(fp, 'w', encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=4)


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
        case _:
            print("指令不存在")
