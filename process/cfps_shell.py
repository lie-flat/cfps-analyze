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

def get_primary_key(k,year):
    if "adult" in k or 'child' in k or 'person' in k or 'famroster' in k:
        return 'pid'
    elif 'famecon' in k:
        match year:
            case 2016:
                return 'fid16'
            case 2018:
                return 'fid18'
            case 2014:
                return 'fid14'
            case 2012:
                return 'fid12'
            case 2010:
                return 'fid'
    elif 'family' in k:
        return 'fid'
    elif 'comm' in k:
        match year:
            case 2014:
                return 'cid14'
            case 2010:
                return 'cid'
    elif 'famconf' in k:
        return 'pid', get_primary_key('famecon',year)
    
class StataDetail:
    def __init__(self, year, vk):
        self.year = year
        self.path = f"{get_data_dir(year)}/Data/Stata/cfps{year}{vk}.dta"
        self.schema = read_json(self.path + ".schemas.json")
        self.key = vk
        self._data = None
        self.primary = get_primary_key(vk, year)

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
