import pandas as pd
import json
import os
import sys

BASIC_MAPPING_A = {-10: '无法判断', -9: '缺失', -8: '不适用', -2: '拒绝回答', -1: '不知道'}
BASIC_MAPPING_B = {-9: '缺失', -8: '不适用', -2: '拒绝回答', -1: '不知道'}


def is_basic_mapping(mapping):
    return mapping == BASIC_MAPPING_A or mapping == BASIC_MAPPING_B


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


def write_sta_file_variable_schemas(reader, df, sta_path):
    print(f"Processing: {sta_path}")
    with open(sta_path + '.schemas.json', 'w', encoding='utf-8') as f:
        labels = reader.variable_labels()
        vlabels = reader.value_labels()
        schemas = {key: {
            'type': 'enum' if not is_basic_mapping(vlabels.get(key, BASIC_MAPPING_A)) else str(df[key].dtype),
            'key': labels[key],
            'range': convert_numpy_indexed_dict_to_serializable_dict(vlabels[key]) if not is_basic_mapping(vlabels.get(key, BASIC_MAPPING_A))
            else list(v.item() if df[key].dtype.kind == 'i' else v for v in df[key].unique()),
            'details': []
        } for key in labels}
        for x in schemas:
            value = schemas[x]
            if value['type'] == 'float64':
                if all(num.is_integer() for num in value['range']):
                    value['range'] = [int(x) for x in value['range']]
                    value['type'] = 'int32' if max(value['range']) < 2147483648 else 'int64'
            if len(value['range']) == len(df[x]):
                value['range'] = None
                value['details'].append(f"{x} 取值各不相同，疑似 ID，已自动省略 range")
            elif len(value['range']) > 500 and value['type'] != 'enum':
                value['range'] = {"min": min(
                    value['range']), "max": max(value['range'])}
                value['details'].append(f"{x} 取值多于 500 个，已转换 range 为最大、最小值")
        json.dump(schemas, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    version = sys.version_info
    if version.major != 3 or version.minor < 10:
        print(
            f"错误：不兼容的 Python 版本： {version.major}.{version.minor}\n请使用 Python 3.10 或更高版本来运行此脚本")
        sys.exit(1)
    if len(sys.argv) != 3:
        print("错误！参数不足。")
        sys.exit(1)
    dir_path = sys.argv[2]
    if not os.path.isdir(dir_path):
        print("错误！指定的目录不存在。")
        sys.exit(1)
    match sys.argv[1]:
        case "gen-var-label":
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
