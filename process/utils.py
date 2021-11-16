import json
import sys


def read_json(fp):
    with open(fp, encoding="utf-8") as f:
        return json.load(f)


def write_json(obj, fp):
    with open(fp, 'w', encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=4)


def require_python_310():
    version = sys.version_info
    if version.major != 3 or version.minor < 10:
        print(
            f"错误：不兼容的 Python 版本： {version.major}.{version.minor}\n请使用 Python 3.10 或更高版本来运行此脚本")
        sys.exit(1)
