# CFPS Query Language Specification

CFPS 查询语言规范。

## Introduction

CFPS 各年数据有所差异，CFPSQL 为一门简化跨年 CFPS 数据查询的查询语言。

```cfpsql
from 2014 to 2016 in adult
region west
type urban
query variable
store in result
visualize by barplot
```