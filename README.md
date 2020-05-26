# AI_spark
专题汇报
### Hadoop+Mapreduce+Hive


MapReduce最早是由Google公司研究提出的一种面向大规模数据处理的并行计算模型和方法。Google公司设计MapReduce的初衷主要是为了解决其搜索引擎中大规模网页数据的并行化处理。Google公司发明了MapReduce之后首先用其重新改写了其搜索引擎中的Web文档索引处理系统。
- 其编程模型只包含map和reduce两个过程，map的主要输入是一对<key , value>值，经过map计算后输出一对<key , value>值；然后将相同key合并，形成<key , value集合>；再将这个<key , value集合>输入reduce，经过计算输出零个或多个<key , value>对。
