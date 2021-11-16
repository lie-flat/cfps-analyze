import sys
import os
from functools import reduce
from itertools import combinations
from cfps_shell import cfps
from utils import read_json, write_json
import re


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


class WrapperSet:
    def __init__(self, *args):
        self._values = []
        for arg in args:
            self._values.extend(arg)
        self.clean()

    def remove_by_identity(self, v):
        """This function is needed since list.remove checks equality instead of identity."""
        for i in range(len(self._values)):
            if self._values[i] is v:
                self._values.pop(i)
                return
        raise ValueError("Can't remove non-existent value!")

    def clean(self):
        eq = {}
        for vid, v in enumerate(self._values):
            for uid, u in enumerate(self._values):
                if uid != vid and u == v:
                    if eq.get(v.value):
                        if u not in eq[v.value]:
                            eq[v.value].append(u)
                        if v not in eq[v.value]:
                            eq[v.value].append(v)
                    else:
                        eq[v.value] = [u, v]
        for k in eq:
            eq[k][0].merge(eq[k][1:])
            for useless in eq[k][1:]:
                self.remove_by_identity(useless)

    def intersection(self, other):
        li = []
        for v in self._values:
            for u in other._values:
                if v == u:
                    li.append(v)
                    li.append(u)
        return WrapperSet(li)

    def dict(self):
        return {v.value: list(v.infos) for v in self._values}

    @classmethod
    def from_schema(cls, year, schema):
        return cls(Wrapper(year, key, schema[key]["key"]) for key in schema)


def transform_value_string(value: str):
    return re.sub(r"[“” ：（）《》<>「」？、【】()，。]", "", value)


class Wrapper:
    def __init__(self, year, key, value):
        self.infos = {(year, key, value)}
        self.value = transform_value_string(value)

    def merge(self, *args):
        for arg in args:
            for w in arg:
                self.infos.update(w.infos)

    def __eq__(self, other):
        return self.value == other.value

    def __repr__(self):
        return f'Wrapper("{re.escape(self.value)}", infos="{self.infos}")'


def analyze_cross_year_in_depth(enabled_tables=None):
    adult = {x: WrapperSet.from_schema(x, cfps[x]["adult"].schema) for x in range(2010, 2017, 2)}
    adult[2018] = WrapperSet.from_schema(2018, cfps[2018].person.schema)
    child = {x: WrapperSet.from_schema(x, cfps[x]["child"].schema) for x in range(2010, 2017, 2)}
    child[2018] = WrapperSet.from_schema(2018, cfps[2018].childproxy.schema)
    comm = {2010: WrapperSet.from_schema(2010, cfps[2010].comm.schema),
            2014: WrapperSet.from_schema(2014, cfps[2014].comm.schema)}
    famecon = {x: WrapperSet.from_schema(x, cfps[x]["famecon"].schema) for x in range(2010, 2019, 2)}
    famconf = {x: WrapperSet.from_schema(x, cfps[x]["famconf"].schema) for x in range(2010, 2019, 2)}

    def analyze_combination(schemas, ncom):
        print(f"Analyze {ncom} combinations")
        r = {}
        for com in combinations(schemas.keys(), ncom):
            cmmn = reduce(WrapperSet.intersection, (schemas[year] for year in com))
            r[reduce(lambda x, y: f"{x}|{y}", com)] = cmmn.dict()
        return r

    def get_result(var_tuple):
        var, var_value = var_tuple
        print(f"Analyze {var}")
        return var, {x: analyze_combination(var_value, x) for x in range(2, len(var_value) + 1)}

    plocals = locals()
    args = list(map(lambda x: (x, plocals[x]), enabled_tables or ("adult", "child", "comm", "famecon", "famconf")))
    # print("Started multiprocessing, 5 workers.")
    # with Pool(5) as p:
    #     results = p.map(get_result, args)
    # for var in ("adult", "child", "comm", "famecon", "famconf"):
    #     value = locals()[var]
    #     write_json({x: analyze_combination(value, x) for x in range(2, len(value) + 1)}, f"docs/{var}_in_depth.json")
    #     print(f"Finished analysis for {var}.")
    for var, value in map(get_result, args):
        write_json(value, f"docs/{var}_in_depth.json")


def analyze_cross_year():
    adult = {x: cfps[x]["adult"].schema for x in range(2010, 2017, 2)}
    adult[2018] = cfps[2018].person.schema
    child = {x: cfps[x]["child"].schema for x in range(2010, 2017, 2)}
    child[2018] = cfps[2018].childproxy.schema
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
        case "cross-year-in-depth":
            analyze_cross_year_in_depth(sys.argv[2:] if len(sys.argv) > 2 else None)
            print("OK")
        case _:
            print("指令不存在")
