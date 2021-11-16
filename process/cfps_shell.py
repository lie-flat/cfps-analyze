from utils import read_json, write_json
from stata_converter import convert_sta_file_to_df
import types
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from functools import reduce

from data_format import datafmt
from constants import REGIONS


def set_global(key, value):
    globals()[key] = value


def get_data_dir(year):
    return f"dataset/CFPS {year}"


def get_primary_key(k, year):
    if "adult" in k or 'child' in k or 'person' in k or 'famroster' in k:
        return 'pid'
    elif 'famecon' in k:
        if year == 2010:
            return 'fid'
        else:
            return f'fid{str(year)[-2:]}'
    elif 'family' in k:
        return 'fid'
    elif 'comm' in k:
        match year:
            case 2014:
                return 'cid14'
            case 2010:
                return 'cid'
    elif 'famconf' in k:
        return 'pid', get_primary_key('famecon', year)


class StataDetail:
    def __init__(self, year, vk):
        self.year = year
        self.path = f"{get_data_dir(year)}/Data/Stata/cfps{year}{vk}.dta"
        self.schema = read_json(self.path + ".schemas.json")
        self.key = vk
        self._data = None
        self._rural = None
        self._urban = None
        self.primary = get_primary_key(vk, year)
        self.compute_urban = self.__rural_urban_compute_function_generator(
            "urban", 1)
        self.compute_rural = self.__rural_urban_compute_function_generator(
            "rural", 0)

    def __repr__(self):
        return f"StataDetail({self.year}, {self.key}, primary:{self.primary})"

    def __get_provcd_index(self):
        if self.year <= 2012:
            return "provcd"
        else:
            return f"provcd{str(self.year)[-2:]}"

    def __getitem__(self, key):
        if isinstance(key, str):
            if key == "data":
                return self.data
            if key in REGIONS:
                return self.data[self.data[self.__get_provcd_index()].isin(REGIONS[key])]
            if key in {"urban", "rural"}:
                return self.rural if key == "rural" else self.urban
        if isinstance(key, tuple):
            if len(key) != 2:
                raise ValueError(f"Invalid key: {key}")

            def combine_urban_rural_and_region(r, key):
                if key not in {"urban", "rural"}:
                    raise ValueError(f"Invalid key: {key}")
                if r in REGIONS:
                    return getattr(self, key)[getattr(self, key)[self.__get_provcd_index()].isin(REGIONS[r])]
                else:
                    raise ValueError(f"Invalid region: {r}")

            return combine_urban_rural_and_region(*key)

    def __rural_urban_compute_function_generator(self, prefix, target_value):
        def compute():
            if "famconf" in self.key:
                raise NotImplementedError(
                    f"{prefix} computation not implemented for famconf.")
            elif "famroster" in self.key:
                raise NotImplementedError(
                    f"{prefix} computation not implemented for famroster.")
            else:
                if self.year < 2012:
                    return self.data[self.data["urban"] == target_value]
                else:
                    return self.data[self.data[f"urban{str(self.year)[-2:]}"] == target_value]

        return compute

    @property
    def rural(self):
        if self._rural is None:
            self._rural = self.compute_rural()
        return self._rural

    @property
    def urban(self):
        if self._urban is None:
            self._urban = self.compute_urban()
        return self._urban

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

    def __len__(self):
        return len(self.__dict__)


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
