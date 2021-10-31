# CFPS 数据处理 (2021 年秋大作业)

## 目录结构

请将数据文件按照以下方式存放：

数据集按下列树形结构放在 `dataset` 文件夹下

```
dataset
├─CFPS 2010
│  │
│  ├─Data
│  │  └─Stata
│  │          cfps2010adult_202008.dta
│  │          cfps2010child_201906.dta
│  │          cfps2010comm_201906.dta
│  │          cfps2010famconf_202008.dta
│  │          cfps2010famecon_202008.dta
│  │
│  └─Documentation
│          家庭关系库.pdf
│          家庭问卷库.pdf
│          少儿问卷库.pdf
│          成人问卷库.pdf
│          社区问卷库.pdf
│          问卷.pdf
│
├─CFPS 2011
│  │
│  ├─Data
│  │  └─Stata
│  │          cfps2011adult_102014.dta
│  │          cfps2011child_102014.dta
│  │          cfps2011family_202008.dta
│  │          cfps2011famroster_202008.dta
│  │
│  └─Documentation
│          6b1bb40d683b405e9b7ed1e8329c1e65.pdf
│
├─CFPS 2012
│  │
│  ├─Data
│  │  └─Stata
│  │          cfps2012adult_201906.dta
│  │          cfps2012child_201906.dta
│  │          cfps2012famconf_092015.dta
│  │          cfps2012famecon_201906.dta
│  │
│  └─Documentation
│          CFPS2012codebook(少儿问卷).xls
│          家庭关系库.pdf
│          家庭经济库.pdf
│          成人问卷.pdf
│          跨年id库.pdf
│          问卷.pdf
│
├─CFPS 2014
│  │
│  ├─Data
│  │  └─Stata
│  │          cfps2014adult_201906.dta
│  │          cfps2014child_201906.dta
│  │          cfps2014comm_201906.dta
│  │          cfps2014famconf_170630.dta
│  │          cfps2014famecon_201906.dta
│  │
│  └─Documentation
│          019b4fced85d4e42a825c3a186695155.pdf
│          CFPS2014codebook.xls
│
├─CFPS 2016
│  │
│  ├─Data
│  │  └─Stata
│  │          cfps2016adult_201906.dta
│  │          cfps2016child_201906.dta
│  │          cfps2016famconf_201804.dta
│  │          cfps2016famecon_201807.dta
│  │
│  └─Documentation
│          CFPS2016codebook.xls
│          问卷.pdf
│
├─CFPS 2018
│  │
│  ├─Data
│  │  └─Stata
│  │          cfps2018childproxy_202012.dta
│  │          cfps2018crossyearid_202104.dta
│  │          cfps2018famconf_202008.dta
│  │          cfps2018famecon_202101.dta
│  │          cfps2018person_202012.dta
│  │
│  └─Documentation
│          CFPS2018codebook.xlsx
│          crossyearid_codebook.xlsx
│          问卷.pdf
│
└─教学数据集
        onlinedemo.dta
```

## 数据初步处理

### 依赖安装

- 在安装依赖之前，请确认你所使用的 Python 版本不低于 3.10
- ipython 为推荐依赖项，也可以不安装

```bash
pip install -r process/requirements.txt
pip install ipython
```

### 探索数据

#### 生成 Schemas

在 Repo 根目录下运行以下命令可以生成 schemas:

(生成的 *.schemas.json 存放在对应的 *.dta 文件旁边)

```bash
python process/stata_converter.py gen-schemas dataset
```

您也可以运行以下命令导出变量表：

```bash
python process/stata_converter.py gen-labels dataset
```

另外，您也可以导出 csv 文件（Experimental， 不保证导出数据的质量）：

```bash
python process/stata_converter.py gen-csv dataset
```

#### 交互式 Shell

```bash
ipython -i process/cfps_shell.py
```

此交互式 Shell 已经默认 `import` 了 `numpy`, `pandas`, `matplotlib` 等库。

使用此交互式 Shell 前，请确保已经生成了 `schemas`

在此 Shell 中，您可以使用 `cfps` 全局变量来访问 `cfps` 数据，您也可以使用 `cfps年份` 这样的全局变量来访问对应年份的数据。

```python
In [2]: cfps2011
Out[2]: 
namespace(adult=<__main__.StataDetail at 0x1f89da19db0>,
          child=<__main__.StataDetail at 0x1f89da19ea0>,
          family=<__main__.StataDetail at 0x1f89da19f00>,
          famroster=<__main__.StataDetail at 0x1f89da19f30>)
In [3]: cfps[2012]
Out[3]: 
namespace(adult=<__main__.StataDetail at 0x1f89da19ba0>,
          child=<__main__.StataDetail at 0x1f89da19f90>,
          famconf=<__main__.StataDetail at 0x1f89da19ff0>,
          famecon=<__main__.StataDetail at 0x1f89da19fc0>)
```

然后，您可以通过各个字段访问数据和元数据。

```python
cfps2011.adult.year # 2011
cfps2011.adult.key # adult_102014
cfps2011.adult.primary # pid
cfps2011.adult.path # 'dataset/CFPS 2011/Data/Stata/cfps2011adult_102014.dta'
cfps2011.adult.schema # 返回 Schema 字典
cfps2011.adult.data # 返回 Pandas DataFrame (Lazy load)
```

### 直接写入数据库

为应用程序创建 MySQL 数据库账户, 然后修改 `process/mysql_storage.py` 中的数据库连接配置。

运行以下命令初始化数据库

```sh
python process/mysql_storage.py db init
```

运行以下命令将所有数据写入数据库：

（注意：你需要先生成 schemas 文件，参见上文）

```sh
python process/mysql_storage.py db write
```

在写入数据库时，应用程序会显示进度条：

```output
.......
100%|███████████████████████████████████████████████████████████████| 58504/58504 [04:17<00:00, 227.63it/s]
Creating famecon_2018...
100%|███████████████████████████████████████████████████████████████| 14218/14218 [01:06<00:00, 212.51it/s]
```

如果需要 dry-run, 只需要把上述命令中的 db 替换为 dry

#### 高级用法

将 `start_year` 到 `end_year` (闭区间) 的数据入库

```sh
python process/mysql_storage.py db write [start_year=2010] [end_year=2018]
```

将指定表的数据入库

```sh
python process/mysql_storage.py db write-one <year> <table_base_name>
```

例如

```sh
python process/mysql_storage.py db write-one 2011 adult
```

