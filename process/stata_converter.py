import pandas as pd
import json
import os
import sys
from utils import require_python_310

BASIC_MAPPING_A = {-10: '无法判断', -9: '缺失', -8: '不适用', -2: '拒绝回答', -1: '不知道'}
BASIC_MAPPING_B = {-9: '缺失', -8: '不适用', -2: '拒绝回答', -1: '不知道'}
BASIC_MAPPINGS = [BASIC_MAPPING_A, BASIC_MAPPING_B,  {
    -10: "无法判断",
    -9: "缺失",
    -8: "不适用",
    -2: "拒绝回答",
    -1: "不知道",
    1: "是",
    5: "否",
    79: "情况不适用"
}, {
    -10: "无法判断",
    -9: "缺失",
    -8: "不适用",
    -2: "拒绝回答",
    -1: "不知道",
    77: "其他",
    78: "以上都没有",
    79: "没有"
}, ]


def is_basic_mapping(mapping):
    # Do not use `in` here. Check equality instead of references.
    for m in BASIC_MAPPINGS:
        if mapping == m:
            return True
    return False


def convert_sta_file_to_reader(file_path):
    stata = pd.read_stata(file_path, iterator=True)
    return stata


def convert_sta_file_to_df(file_path):
    stata = pd.read_stata(file_path, convert_categoricals=False)
    return stata


def write_sta_file_variable_labels(reader, sta_path):
    with open(sta_path + '.labels.json', 'w', encoding='utf-8') as f:
        json.dump(reader.variable_labels(), f, ensure_ascii=False, indent=4)


def convert_numpy_indexed_dict_to_serializable_dict(dic):
    return {key.item(): dic[key] for key in dic}


def any_number_not_in_enum_range(df_col, range):
    return any(num not in range and not pd.isna(num) for num in df_col)

def convert_to_serializable_num(n):
    return n.item() if 'item' in dir(n) else n

def write_sta_file_variable_schemas(reader, df, sta_path):
    print(f"Processing: {sta_path}")
    with open(sta_path + '.schemas.json', 'w', encoding='utf-8') as f:
        labels = reader.variable_labels()
        vlabels = reader.value_labels()
        schemas = {key: {
            'type': 'enum' if not is_basic_mapping(vlabels.get(key, BASIC_MAPPING_A)) else str(df[key].dtype),
            'key': labels[key],
            'range': convert_numpy_indexed_dict_to_serializable_dict(vlabels[key]) if not is_basic_mapping(vlabels.get(key, BASIC_MAPPING_A))
            else list(v.item() if 'item' in dir(v) else v for v in df[key].unique()),
            'details': [],
            'minmax': list(map(convert_to_serializable_num,[df[key].min(), df[key].max()])) if df[key].dtype != 'object' else None
        } for key in labels}
        for x in schemas:
            value = schemas[x]
            if value['type'] == 'float64':
                if all(num.is_integer() for num in value['range']):
                    value['range'] = [int(x) for x in value['range']]
                    value['type'] = 'int32'
            if len(value['range']) == len(df[x]):
                value['range'] = {"min": min(
                    value['range']), "max": max(value['range'])}
                value['details'].append(f"{x} 取值各不相同，疑似 ID，已转换 range 为最大、最小值")
            elif len(value['range']) > 500 and value['type'] != 'enum':
                value['range'] = {"min": min(
                    value['range']), "max": max(value['range'])}
                value['details'].append(f"{x} 取值多于 500 个，已转换 range 为最大、最小值")
        json.dump(schemas, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    require_python_310()
    if len(sys.argv) != 3:
        print("错误！参数不足。")
        sys.exit(1)
    dir_path = sys.argv[2]
    if not os.path.isdir(dir_path):
        print("错误！指定的目录不存在。")
        sys.exit(1)
    match sys.argv[1]:
        case "gen-labels":
            for root, dirs, files in os.walk(dir_path):
                for file in files:
                    if file.endswith(".dta"):
                        file_path = os.path.join(root, file)
                        reader = convert_sta_file_to_reader(file_path)
                        write_sta_file_variable_labels(reader, file_path)
            print("成功！")
        case "gen-csv":
            for root, dirs, files in os.walk(dir_path):
                for file in files:
                    if file.endswith(".dta"):
                        file_path = os.path.join(root, file)
                        df = convert_sta_file_to_df(file_path)
                        df.to_csv(file_path + ".csv")
            print("成功！")
        case "gen-schemas":
            for root, dirs, files in os.walk(dir_path):
                for file in files:
                    if file.endswith(".dta"):
                        file_path = os.path.join(root, file)
                        df = convert_sta_file_to_df(file_path)
                        reader = convert_sta_file_to_reader(file_path)
                        write_sta_file_variable_schemas(reader, df, file_path)
            print("成功！")
