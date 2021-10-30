from utils import read_json, write_json
from stata_converter import convert_sta_file_to_df
import types
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from functools import reduce


from data_format import datafmt


def set_global(key, value):
    globals()[key] = value


def get_data_dir(year):
    return f"dataset/CFPS {year}"


class StataDetail:
    def __init__(self, year, vk):
        self.year = year
        self.path = f"{get_data_dir(year)}/Data/Stata/cfps{year}{vk}.dta"
        self.schema = read_json(self.path + ".schemas.json")
        self._data = None

    @property
    def data(self):
        if self._data is None:
            self._data = convert_sta_file_to_df(self.path)
        return self._data


class ObjectDict(types.SimpleNamespace):
    def __getitem__(self, k):
        return self.__dict__[k]

    def __iter__(self):
        return iter(self.__dict__)


def make_data_obj(year, value):
    obj = ObjectDict()
    obj.__dict__.update({
        value[vk]: StataDetail(year, vk)
        for vk in value
    })
    return obj


cfps = {}

for year in datafmt:
    set_global(f"cfps{year}", make_data_obj(year, datafmt[year]))
    cfps[year] = globals()[f"cfps{year}"]
