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

- 建议使用 Linux 操作系统
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
namespace(adult=StataDetail(2011, adult_102014, primary:pid),
          child=StataDetail(2011, child_102014, primary:pid),
          family=StataDetail(2011, family_202008, primary:fid),
          famroster=StataDetail(2011, famroster_202008, primary:pid))
In [3]: cfps[2012]
Out[3]: 
namespace(adult=StataDetail(2012, adult_201906, primary:pid),
          child=StataDetail(2012, child_201906, primary:pid),
          famconf=StataDetail(2012, famconf_092015, primary:('pid', 'fid12')),
          famecon=StataDetail(2012, famecon_201906, primary:fid12))
```

然后，您可以通过各个字段访问数据和元数据。

```python
# 可以用数组形式访问对应年的数据
cfps[2011].adult.year # 2011
# 也可以直接用变量名访问
cfps2011.adult.key # adult_102014
cfps2011.adult.primary # pid
cfps2011.adult.path # 'dataset/CFPS 2011/Data/Stata/cfps2011adult_102014.dta'
cfps2011.adult.schema # 返回 Schema 字典
cfps2011.adult.data # 返回 Pandas DataFrame (Lazy load)
cfps2012.adult.rural # 返回乡村地区数据
cfps2012.adult.urban # 返回城镇地区数据
# 也可以以索引的形式访问
cfps2012.adult["urban"] # 返回城镇地区数据
cfps2016.child["east"] # 返回东部地区数据
cfps2016.child["west"] # 返回西部地区数据
cfps2018.person["west", "rural"] # 返回西部乡村数据
cfps2018.person["northeast", "urban"] # 返回东北部城镇数据
```

### 直接写入数据库

启动数据库（如已启动，请忽略）, 进入 Mysql Shell

本文假设您在 Linux 环境下操作，Windows 用户请自行执行下面的指令对应的操作。

```sh
sudo systemctl start mysql
sudo mysql
```

为应用程序创建 MySQL 数据库账户, 然后修改 `process/mysql_storage.py` 中的数据库连接配置。

注意： 下列命令给了 cfps 全部权限，在生产环境中请按需修改!

```sql
CREATE USER 'cfps'@'localhost' IDENTIFIED BY 'cfpsMySQL111++';
GRANT ALL PRIVILEGES ON * . * TO 'cfps'@'localhost';
FLUSH PRIVILEGES;
```

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
Creating famecon_2010...
100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 14797/14797 [00:28<00:00, 527.86it/s]
Creating adult_2011...
100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 1279/1279 [00:03<00:00, 364.06it/s]
Creating child_2011...
100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 7524/7524 [00:16<00:00, 443.37it/s]
Creating family_2011...
100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 13129/13129 [00:13<00:00, 948.86it/s]
Creating famroster_2011...
100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 50954/50954 [00:10<00:00, 4902.50it/s]
Creating adult_2012...
100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 35719/35719 [03:01<00:00, 197.33it/s]
Creating child_2012...
100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 8620/8620 [00:22<00:00, 379.30it/s]
Creating famconf_2012...
100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 55012/55012 [01:07<00:00, 811.34it/s]
Creating famecon_2012...
100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 13315/13315 [00:25<00:00, 515.88it/s]
Creating adult_2014...
100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 37147/37147 [02:13<00:00, 277.24it/s]
Creating child_2014...
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

### 数据筛选与清洗

出于本次大作业要求，我们需要保留原始数据库，因此我们保留了所有原始表格，自己再新建一些表格/视图来存放清洗后的数据

运行以下命令来进行数据筛选，筛选后形成的新表的名称会带有 clean 后缀，如： `adult_2010_clean`

```bash
python process/mysql_storage.py db filter
```

然后，可以运行以下命令来分解部分表格

```bash
python process/mysql_storage.py db decompose <配置文件路径>
```

配置文件的语法如下所示：

decompositions/child.json

```json
{
  "table": "child",
  "postfix": "infant",
  "2012|2014": {
    "condition": "cfps{year}_age<2",
    "columns": [
      "wb8",
      "wf701",
      "wd2",
      "wf603m",
      "wa103",
      "wa105b",
      "wz302",
      "wf605m",
      "wg305",
      "wb701",
      "wg302",
      "wd402",
      "wb401",
      "wb801",
      "wf501",
      "pid"
    ]
  },
  "2010": {
    "condition": "childgroup=1",
    "columns": {
      "pid": "pid",
      "wa101": "孩子的胎龄（月）",
      "wa102": "孩子出生时的体重（斤）",
      "wa103": "孩子现在的体重（斤）",
      "wa104": "孩子现在的身高（厘米）"
    }
  },
  "2016|2018": {
    "condition": {
      "2016": "cfps_age<2",
      "2018": "age<2"
    },
    "columns": [
      "wb8",
      "wf701",
      "wd2",
      "wf603m",
      "wa103",
      "wa105b",
      "wz302",
      "wf605m",
      "wg305",
      "wb701",
      "wg302",
      "wd402",
      "wb401",
      "wb801",
      "wf501",
      "pid"
    ]
  }
}
```

配置文件的语法非常灵活，您需要把要处理的表格名称存在 `table` 中，后缀存到 `postfix` 中，这样生成的新表格的名称就是 `child_2010_infant` 这样子的.

当一个设置适用于多年时，您可以将各年以 `|` 分隔作为键。

当多个年的所选数据相同，而查询条件不同时，您也可以将 `condition` 写成一个字典。

另外，我们支持在 `condition` 中插值，您可以使用 `{year}` 这样的插值字符串来处理诸如 `cfps2012_age` 这种变量。

您应该将需要选取的变量书写在 `columns` 中，无论是列表还是字典，我们的程序都能妥善处理
