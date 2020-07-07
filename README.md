# 数据生产力工具及其转变
## Mapreduce

### Mapreduce算法基本原理

MapReduce最早是由Google公司研究提出的一种面向大规模数据处理的并行计算模型和方法（2003年论文）。Google公司设计MapReduce的初衷主要是为了解决其搜索引擎中大规模网页数据的并行化处理。Google公司发明了MapReduce之后首先用其重新改写了其搜索引擎中的Web文档索引处理系统。

![image](https://github.com/Xiezhibin/AI_spark/blob/master/images/MapReduce%E4%BD%9C%E4%B8%9A%E6%B5%81%E7%A8%8B.png)

- 作业启动：开发者通过控制台启动作业；
- 作业初始化：这里主要是切分数据、创建作业和提交作业，与第三步紧密相联；
- 作业/任务调度：对于2.0版的Hadoop来说就是Yarn中的Resource Manager负责整个系统的资源管理与分配；
- Map任务；数据输入,做初步的处理,输出形式的中间结果；
- Shuffle：按照partition、key对中间结果进行排序合并,输出给reduce线程；
- Reduce任务：对相同key的输入进行最终的处理,并将结果写入到文件中；
- 作业完成：通知开发者任务完成。

根据官网给出的流程图，可以看出两个环节都涉及到数据读取、分区、溢写、合并、计算这几个环节。

#### Map操作

1. 在Map Task任务的业务处理方法map()中，最后一步通过OutputCollector.collect(key,value)或context.write(key,value)输出Map Task的中间处理结果，在相关的collect(key,value)方法中，会调用Partitioner.getPartition(K2 key, V2 value, int numPartitions)方法获得输出的key/value对应的分区号(分区号可以认为对应着一个要执行Reduce Task的节点)，然后将<key,value,partition>暂时保存在内存中的MapOutputBuffe内部的环形数据缓冲区，该缓冲区的默认大小是100MB，可以通过参数io.sort.mb来调整其大小。
2. 当缓冲区中的数据使用率达到一定阀值后，触发一次Spill操作，将环形缓冲区中的部分数据写到磁盘上，生成一个临时的Linux本地数据的spill文件；然后在缓冲区的使用率再次达到阀值后，再次生成一个spill文件。直到数据处理完毕，在磁盘上会生成很多的临时文件。
3. 缓存有一个阀值比例配置，当达到整个缓存的这个比例时，会触发spill操作；触发时，map输出还会接着往剩下的空间写入，但是写满的空间会被锁定，数据溢出写入磁盘。当当这部分溢出的数据写完后，空出的内存空间可以接着被使用，形成像环一样的被循环使用的效果，所以又叫做环形内存缓冲区。

#### Reduced 操作
1. 拉取数据；Reduce进程启动一些数据copy线程(Fetcher)，通过HTTP方式请求map task所在的TaskTracker获取map task的输出文件。因为这时map task早已结束，这些文件就归TaskTracker管理在本地磁盘中;
2. 内存缓冲;
3. merge过程,Copy过来的数据会先放入内存缓冲区中,当内存中的数据量到达一定阈值，就启动内存到磁盘的 merge,当map端的数据传输结束时候时候，会进行磁盘到磁盘的merge操作

![image](https://github.com/Xiezhibin/AI_spark/blob/master/images/mapreduce.png)

我们用一段伪代码来观察一下mapreduce过程的流程：
```
SELECT age, AVG(contacts)
    FROM social.person
GROUP BY age
ORDER BY age
```
改sql的运行机理通过map和reduce两步进行操作。
```
function Map is
    input: integer K1 between 1 and 1100, representing a batch of 1 million social.person records
    for each social.person record in the K1 batch do
        let Y be the person's age
        let N be the number of contacts the person has
        produce one output record (Y,(N,1))
    repeat
end function

function Reduce is
    input: age (in years) Y
    for each input record (Y,(N,C)) do
        Accumulate in S the sum of N*C
        Accumulate in Cnew the sum of C
    repeat
    let A be S/Cnew
    produce one output record (Y,(A,Cnew))
end function
```
### Mapreduce缺点
#### 高昂的维护成本
使用 MapReduce，你需要严格地遵循分步的 Map 和 Reduce 步骤。当你构造更为复杂的处理架构时，往往需要协调多个 Map 和多个 Reduce 任务。然而，每一步的 MapReduce 都有可能出错。
#### 时间性能“达不到”用户的期待
Google 曾经在 2007 年到 2012 年间做过一个对于 1PB 数据的大规模排序实验，来测试 MapReduce 的性能。从 2007 年的排序时间 12 小时，到 2012 年的排序时间缩短至 0.5 小时。即使是 Google，也花了 5 年的时间才不断优化了一个 MapReduce 流程的效率。2011 年，他们在 Google Research 的博客上公布了初步的成果。其中有一个重要的发现，就是他们在 MapReduce 的性能配置上花了非常多的时间。包括了缓冲大小 (buffer size），分片多少（number of shards），预抓取策略（prefetch），缓存大小（cache size）等等。

![image](https://github.com/Xiezhibin/AI_spark/blob/master/images/drawback1.png)

选择一个好的分片函数（sharding function）为何格外重要？让我们来看一个例子。假如你在处理 Facebook 的所有用户数据，你选择了按照用户的年龄作为分片函数（sharding function）。我们来看看这时候会发生什么。因为用户的年龄分布不均衡（假如在 20~30 这个年龄段的 Facebook 用户最多），导致我们在下图中 worker C 上分配到的任务远大于别的机器上的任务量。

![image](https://github.com/Xiezhibin/AI_spark/blob/master/images/drawback2.png)

这时候就会发生掉队者问题（stragglers）。别的机器都完成了 Reduce 阶段，只有 worker C 还在工作。当然它也有改进方法。掉队者问题可以通过 MapReduce 的性能剖析（profiling）发现,如下图所示，箭头处就是掉队的机器。

![image](https://github.com/Xiezhibin/AI_spark/blob/master/images/drawback3.png)
>Chen, Qi, Cheng Liu, and Zhen Xiao. “Improving MapReduce performance using smart speculative execution strategy.” IEEE Transactions on Computers 63.4 (2014): 954-967.

1. MapReduce 模型的抽象层次低，大量的底层逻辑都需要开发者手工完成。

2. 只提供 Map 和 Reduce 两个操作。很多现实的数据处理场景并不适合用这个模型来描述。实现复杂的操作很有技巧性，也会让整个工程变得庞大以及难以维护。举个例子，两个数据集的 Join 是很基本而且常用的功能，但是在 MapReduce 的世界中，需要对这两个数据集做一次 Map 和 Reduce 才能得到结果。这样框架对于开发者非常不友好。正如第一讲中提到的，维护一个多任务协调的状态机成本很高，而且可扩展性非常差。

3. 在 Hadoop 中，每一个 Job 的计算结果都会存储在 HDFS 文件存储系统中，所以每一步计算都要进行硬盘的读取和写入，大大增加了系统的延迟。由于这一原因，MapReduce 对于迭代算法的处理性能很差，而且很耗资源。因为迭代的每一步都要对 HDFS 进行读写，所以每一步都需要差不多的等待时间。

4. 只支持批数据处理，欠缺对流数据处理的支持。

## Apache spark
spark是一个实现快速通用的集群计算平台。它是由加州大学伯克利分校AMP实验室 开发的通用内存并行计算框架，用来构建大型的、低延迟的数据分析应用程序。它扩展了广泛使用的MapReduce计算模型。高效的支撑更多计算模式，包括交互式查询和流处理。spark的一个主要特点是能够在内存中进行计算，及时依赖磁盘进行复杂的运算，Spark依然比MapReduce更加高效。Spark 最基本的数据抽象叫作弹性分布式数据集（Resilient Distributed Dataset, RDD），它代表一个可以被分区（partition）的只读数据集，它内部可以有很多分区，每个分区又有大量的数据记录（record)。

### spark优点


1. Spark 提供了很多对 RDD 的操作，如 Map、Filter、flatMap、groupByKey 和 Union 等等，极大地提升了对各种复杂场景的支持。开发者既不用再绞尽脑汁挖掘 MapReduce 模型的潜力，也不用维护复杂的 MapReduce 状态机。

2. 相对于 Hadoop 的 MapReduce 会将中间数据存放到硬盘中，Spark 会把中间数据缓存在内存中，从而减少了很多由于硬盘读写而导致的延迟，大大加快了处理速度。由于 Spark 可以把迭代过程中每一步的计算结果都缓存在内存中，所以非常适用于各类迭代算法。Spark 第一次启动时需要把数据载入到内存，之后的迭代可以直接在内存里利用中间结果做不落地的运算。所以，后期的迭代速度快到可以忽略不计。在当今机器学习和人工智能大热的环境下，Spark 无疑是更好的数据处理引擎。

下图是在 Spark 和 Hadoop 上运行逻辑回归算法的运行时间对比。

![image](https://github.com/Xiezhibin/AI_spark/blob/master/images/spark1.png)

可以看出，Hadoop 做每一次迭代运算的时间基本相同，而 Spark 除了第一次载入数据到内存以外，别的迭代时间基本可以忽略。

#### RDD
RDD又称弹性分布式数据集，是spark这座大厦的基础，有以下基本特性：分区、不可变和并行操作。
- 分区：分区代表同一个 RDD 包含的数据被存储在系统的不同节点中，在物理存储中，每个分区指向一个存放在内存或者硬盘中的数据块（Block），而这些数据块是独立的，它们可以被存放在系统中的不同节点。
- 不可变性：代表每一个 RDD 都是只读的，它所包含的分区信息不可以被改变。显然，对于代表中间结果的 RDD，我们需要记录它是通过哪个 RDD 进行哪些转换操作得来，即依赖关系，而不用立刻去具体存储计算出的数据本身。这样做有助于提升 Spark 的计算效率，并且使错误恢复更加容易。
- 并行操作：并行操作由于单个 RDD 的分区特性，使得它天然支持并行操作，即不同节点上的数据可以被分别处理，然后产生一个新的 RDD。

RDD转换操作
RDD 的数据操作分为两种：转换（Transformation）和动作（Action）
```
###  RDD 的转换操作
- Map
- Filter
- mapPartitions
- groupByKey
###  RDD 的动作操作
- Collect
- Reduce
- CountByKey
```
Spark 在每次转换操作的时候，使用了新产生的 RDD 来记录计算逻辑，这样就把作用在 RDD 上的所有计算逻辑串起来，形成了一个链条。当对 RDD 进行动作时，Spark 会从计算链的最后一个 RDD 开始，依次从上一个 RDD 获取数据并执行计算逻辑，最后输出结果。

每当我们对 RDD 调用一个新的 action 操作时，整个 RDD 都会从头开始运算。因此，如果某个 RDD 会被反复重用的话，每次都从头计算非常低效，我们应该对多次使用的 RDD 进行一个持久化操作。Spark 的 persist() 和 cache() 方法支持将 RDD 的数据缓存至内存或硬盘中，这样当下次对同一 RDD 进行 Action 操作时，可以直接读取 RDD 的结果，大幅提高了 Spark 的计算效率。
```
rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd1 = rdd.map(lambda x: x+5)
rdd2 = rdd1.filter(lambda x: x % 2 == 0)
rdd2.persist()
count = rdd2.count() // 3
first = rdd2.first() // 6
rdd2.unpersist()
```

#### DataSet + DataFrame
1. 同弹性分布式数据集类似，DataSet 也是不可变分布式的数据单元，它既有与 RDD 类似的各种转换和动作函数定义，DataSet以Catalyst逻辑执行计划表示，并且数据以编码的二进制形式被存储，不需要反序列化就可以执行sorting、shuffle等操作。
2. DataFrame 可以被看作是一种特殊的 DataSet。它也是关系型数据库中表一样的结构化存储机制，也是分布式不可变的数据结构。

![image](https://github.com/Xiezhibin/AI_spark/blob/master/images/Spark_Sql1.png)

#### Spark SQL

Spark SQL 本质上是一个库。它运行在 Spark 的核心执行引擎之上。

![image](https://github.com/Xiezhibin/AI_spark/blob/master/images/Spark_Sql2.png)

如上图所示，它提供类似于 SQL 的操作接口，允许数据仓库应用程序直接获取数据，允许使用者通过命令行操作来交互地查询数据，还提供两个 API：DataFrame API 和 DataSet API。

Java、Python 和 Scala 的应用程序可以通过这两个 API 来读取和写入 RDD。此外，应用程序还可以直接操作 RDD。使用 Spark SQL 会让开发者觉得好像是在操作一个关系型数据库一样，而不是在操作 RDD。这是它优于原生的 RDD API 的地方。

## Apache Beam
Apache Beam是一个开源统一编程模型，用于定义和执行数据处理管道，包括ETL、批处理和流（连续）处理。[1] Beam流水线是使用提供的SDK之一定义的，并在Beam支持的一个运行器（分布式处理后端）中执行，包括Apache Apex、Apache Flink、Apache Gearpump（孵化中）、Apache Samza、Apache Spark和Google Cloud Dataflow。
![image](https://github.com/Xiezhibin/AI_spark/blob/master/images/Beam1.png)
#### Beam 的编程模型
如下图所示，Beam模型中一共可以分成如下六个部分。

```
第一层，是现在已有的各种大数据处理平台（例如 Apache Spark 或者 Apache Flink），在 Beam 中它们也被称为 Runner。

第二层，是可移植的统一模型层，各个 Runners 将会依据中间抽象出来的这个模型思想，提供一套符合这个模型的 APIs 出来，以供上层转换。

第三层，是 SDK 层。SDK 层将会给工程师提供不同语言版本的 API 来编写数据处理逻辑，这些逻辑就会被转化成 Runner 中相应的 API 来运行。

第四层，是可扩展库层。工程师可以根据已有的 Beam SDK，贡献分享出更多的新开发者 SDK、IO 连接器、转换操作库等等。

第五层，我们可以看作是应用层，各种应用将会通过下层的 Beam SDK 或工程师贡献的开发者 SDK 来实现。

第六层，也就是社区一层。在这里，全世界的工程师可以提出问题，解决问题，实现解决问题的思路。
```

![image](https://github.com/Xiezhibin/AI_spark/blob/master/images/beam2.png)

这个世界中的数据可以分成有边界数据和无边界数据，而有边界数据又是无边界数据的一种特例。所以，我们都可以将所有的数据抽象看作是无边界数据。

同时，每一个数据都是有两种时域的，分别是事件时间和处理时间。我们在处理无边界数据的时候，因为在现实世界中，数据会有延时、丢失等等的状况发生，我们无法保证现在到底是否接收完了所有发生在某一时刻之前的数据。所以现实中，流处理必须在数据的完整性和数据处理的延时性上作出取舍。

Beam 编程模型就是在这样的基础上提出的。Beam 编程模型会涉及到的 4 个概念，窗口、水印、触发器和累加模式，我来为你介绍一下。

##### 窗口（Window）
窗口将无边界数据根据事件时间分成了一个个有限的数据集。我们可以看看批处理这个特例。在批处理中，我们其实是把一个无穷小到无穷大的时间窗口赋予了数据集。

##### 水印（Watermark）
水印是用来表示与数据事件时间相关联的输入完整性的概念。对于事件时间为 X 的水印是指：数据处理逻辑已经得到了所有事件时间小于 X 的无边界数据。在数据处理中，水印是用来测量数据进度的。

##### 触发器（Triggers）
触发器指的是表示在具体什么时候，数据处理逻辑会真正地触发窗口中的数据被计算。触发器能让我们可以在有需要时对数据进行多次运算，例如某时间窗口内的数据有更新，这一窗口内的数据结果需要重算。

##### 累加模式（Accumulation）
累加模式指的是如果我们在同一窗口中得到多个运算结果，我们应该如何处理这些运算结果。这些结果之间可能完全不相关，例如与时间先后无关的结果，直接覆盖以前的运算结果即可。这些结果也可能会重叠在一起。

![image](https://github.com/Xiezhibin/AI_spark/blob/master/images/beam3.png)

#### PCollection

PCollection是Beam的一种特殊的数据结构。
```
- 无序：同样作为数据的容器，PCollection 却并不像 Python/Java 的 List 或者 C++ 的 vector，Beam 对于 PCollection 中元素的处理顺序不作任何保证。
- 无界：PCollection 也不像 Python/Java 的 Set，或者 C++ 的 unordered_set，PCollection 不一定有固定的边界。所以，你也不能指望去查找一个 PCollection 的大小。
- 不可变性：修改一个 PCollection 的唯一方式就是去转化 (Transform) 它。
```

# 建模分析框架及实践

## Lambda Vs Kappa
### Lambda 框架
Lambda 框架（Lambda Architecture）是由 Twitter 工程师南森·马茨（Nathan Marz）提出的大数据处理架构。这一架构的提出基于马茨在 BackType 和 Twitter 上的分布式数据处理系统的经验。Lambda 架构使开发人员能够构建大规模分布式数据处理系统。它具有很好的灵活性和可扩展性，也对硬件故障和人为失误有很好的容错性。Lambda 架构总共由三层系统组成：批处理层（Batch Layer），速度处理层（Speed Layer），以及用于响应查询的服务层（Serving Layer）。

![image](https://github.com/Xiezhibin/AI_spark/blob/master/images/model1.jpg)

### Kappa 框架
虽然 Lambda 框架使用起来十分灵活，并且可以适用于很多的应用场景，但在实际应用的时候，它的维护很复杂。需要维护两个复杂的分布式系统，并且保证他们逻辑上产生相同的结果输出到服务层中。

Kappa 框架是由 LinkedIn 的前首席工程师杰伊·克雷普斯（Jay Kreps）提出的一种架构思想。

```
- 改进速度层的系统性能，使得它可以处理好数据的完整性和准确性问题
- 改进速度层，使它既能进行实时数据处理，也能在业务逻辑更新的情况下重新处理以前处理过的历史数据
```

![image](https://github.com/Xiezhibin/AI_spark/blob/master/images/model2.png)

### 数据建模案例分析
#### 用户点击行为预测
本次一共选取了云闪付注册用户在四季度截止观测期前的交易总金额、红包获取总净额、用户所在城市划分、用户年龄以及通过机器学习方法预测的用户的有房概率以及有车概率作为本次的用户属性特征；主流功能板块点击行为、主流功能板块所属类别的点击行为、以及上日点击占比为云闪付点击行为特征；该行为的引流应用、引流应用所属类别、上日同事件点击次数、上日同类别行为点击次数作为行为序列-用户特征的交叉特征。	

|特征类型        | 变量英文名     | 变量中文名  |
| ------------- |:-------------:| -----:|
| 用户属性特征   | Pre_trans_at  | 交易总金额|
| 用户属性特征   | Pre_point_at  | 红包获取金额|
| 用户属性特征   | Pre_car_prob  | 有车概率 |
| 用户属性特征   |Pre_estate_prob| 有房概率|
| 用户属性特征   | Pre_car_prob |年龄|
用户属性特征   | Pre_city_grd  | 城市发达程度|
| 点击行为特征   | Item_id  | 点击行为id |
| 点击行为特征   |Cate_id| 行为所属类别|
| 点击行为特征   | Last_day_popularcate_ratio|板块点击率|
| 点击行为特征   | last_click_item_id  | 上一点击行为 |
| 行为序列-用户特征交叉特征  |last_click_cate_id| 上一点击行为所属类别|
| 行为序列-用户特征交叉特征   | Last_day_click_the_same_item|上日点击事件次数|
| 行为序列-用户特征交叉特征   | Last_day_click_the_same_cate|昨日点击同类别事件次数|

在创建SparkSession后，用Spark_Sql读取hive表data_tmp.xzb_y_0406中的数据
```
from pyspark.sql import SparkSession,Row, functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import desc
from pyspark.sql.functions import to_date, to_timestamp
import numpy as np
import pandas as pd
import datetime
ss = SparkSession.builder. \
        appName("Daniel_DataMining").enableHiveSupport(). \
        config("spark.yarn.queue", "root.cqpsrvas"). \
        config("spark.sql.hive.convertMetastoreParquet", "True"). \
        config("spark.driver.memory","20G"). \
        config("spark.dynamicAllocation.maxExecutors", "1300"). \
        getOrCreate()
        
f = ss.sql("""select * from data_tmp.xzb_y_0406""")
```
#### 利用spark机器学习包对数据进行预处理

- 在我们想要构建的模型中，有5个输出参数。所以我们需要把它们分离处理；

- 具体的问题有：交易金额和优惠金额的值普遍都很大，我们可以把它们标准化处理；

- 有的属性中有空值，比如昨日点击相同应用，需要我们的转化；

- 有的属性是离散型的数据，比如年龄和所属城市发达水平。需要我们将其他分类处理；

对于第一点，我们可以用select函数和where函数将需要分析的数据取出来

对于第二点，数据的标准化，我们可以借助spark的机器学习库Spark ML来完成。由于Spark ML也是基于DataFrame,很快就可以对数据进行处理。
```
#添加如下5个变量；如果有为1、如果没有为0

def Y_Column(df,names):
    for name in names:
        df=df.withColumn(name,F.when(df['new_item_id']==name,1).otherwise(0))
    return df

columns = ['1', '2', '3', '4', '5', '6']
df = Y_Column(a, columns)

#将数字列变为double存储类型

def convertColumn(df, names, newType):
    for name in names:
        df = df.withColumn(name, df[name].cast(newType))
    return df

columns = ['popularcate_ratio', 'last_click_item', 'last_click_cate_id', 
           'last_day_click_the_same_item', 'last_day_click_the_same_cate', 'pre_estate_prob', 
           'pre_car_prob', 'pre_trans_at','pre_points_at']

df = convertColumn(df, columns, FloatType())

#空值处理
df=df.fillna(0) 

#离散数据处理

df=ss.sql(
"""
select 
case  pre_city_grd  
when  pre_city_grd='二线发展较弱城市' then 1
when  pre_city_grd='三线城市' then 2
when  pre_city_grd is Null then 0
when  pre_city_grd='四线城市' then 3
when  pre_city_grd='一线城市' then 4
when  pre_city_grd='二线发达城市' then 5
when  pre_city_grd='五线城市' then 6
when  pre_city_grd='二线中等城市' then 7
when  pre_city_grd='其他' then 8
else 9
end as pre_city_grd_new,
* from df1
"""
)
df=df.drop(df.pre_city_grd)

# 归一化 
# 把需要所有变量分为feature 和 lable两个部分

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
sembler = VectorAssembler( inputCols=["pre_city_grd_new","popularcate_ratio","pre_estate_prob","pre_car_prob",
"pre_age","pre_trans_at","pre_points_at", "last_click_item", "last_click_cate_id", "last_day_click_the_same_item","last_day_click_the_same_cate"],    outputCol="features")
output = sembler.transform(df2)

label_features1 = output.select("features", "1").toDF('features','label') #这里的1为添加卡的行为操作
### 归一化
from pyspark.ml.feature import StandardScaler

standardScaler = StandardScaler(inputCol="features", outputCol="features_scaled")
scaler = standardScaler.fit(label_features1)
scaled_df = scaler.transform(label_features1)
```
#### 弹性网络（Elastic Net）
```
## 训练集合测试集合选择（一共100万条样本）
train_data, test_data = scaled_df.randomSplit([.8,.2],seed=123)

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol='features_scaled', labelCol="label", maxIter=10, regParam=0.08, elasticNetParam=0.02)
linearModel = lr.fit(train_data)

linearModel.coefficients,linearModel.intercept

#添加卡 1 ==246 popularcate_ratio pre_car_prob pre_trans_at

imp_coef = np.array(linearModel.coefficients)
coef = pd.Series(imp_coef, index = X.columns) 

print("Elastic picked " + str(sum(coef != 0)) + 
      " variables and eliminated the other " +  
      str(sum(coef == 0)) + " variables") 

import matplotlib
imp_coef = pd.concat([coef.sort_values().head(5),
                     coef.sort_values().tail(5)])
matplotlib.rcParams['figure.figsize'] = (6.0, 4.0)
imp_coef.plot(kind = "barh")
plt.title("Coefficients in the Elastic Model")
```
#### 结论
![image](https://github.com/Xiezhibin/AI_spark/blob/master/images/model3.jpg)
##### 云闪付主流功能点击和客群画像存在相关性
大数据和机器学习结果显示，不同的主流应用在特征标准上存在显著差异，云闪付主流功能的使用和用户客族群画像存在强相关。
- 年龄、交易金额、有车特征对“添加卡”功能存在负面效应。通俗来说、用户年龄越大，用户添卡的积极性越低；绑卡之前，也很难存在实质性的交易行为。
- 交易金额对“商城”入口的点击行为有“负项影响”，商城入口对低交易用户的吸引较大。年龄、地域信息未对商场点击产生显著影响。
- 有房用户、红包赢取、交易金额对“付款码”点击有显著的影响，前期第交易用户更有可能使用云闪付的“付款码”功能。年龄、地域分布对“付款码”影响较小。
- 交易金额对“信用卡还款”点击有极其显著的正向影响、“信用卡还款”使用频率较高的用户一般为持续高交易用户。高年龄段用户点击信用卡还款的倾向越高。红包使用用户、有房有车群体的信用卡还款倾向相对偏低。

### spark工具在数据逻辑计算上的优化
由于spark的Transform操作间只涉及到逻辑关系的存储，并未触发实际的计算算子，故而我们在用spark进行数据预处理的时候可以更加灵活的对数据进行处理和分析。

对列变量的处理在数据提取和报表生成中比较常见，实际数据处理的过程中我们常常会遇到对输入表结构进行多次处理生成新的列变量的问题。

比如在信用卡还款的留存分析中，我们想去大致统计**召回策略用户在单个账户在统计月份前贷记卡在总卡中的占比**。其中统一绑卡表的卡片生成逻辑如下，同一用户**usr_id**可能绑定多张卡片，当有新的记录进入时，表字段中的更新时间会发生改变**rec_upd_ts**，以绑卡类型**bind_tp**记录绑定卡片的类型。

不难看出，上述操作中包含有聚合操作、过滤操作、条件选择。如果要用hive数仓进行操作，hql对多次迭代的分析无能为力，需要对中间结果进行存储临时表，而且整个过程需要操作者自己进行优化，涉及到大量的I/O和网络开销。但如果引入SPARK的**DataFrame API**，只需要很少的代码就可以高效完成上述任务。
```
#读取召回策略用户，并存储于缓存之中
f=ss.sql("select * from data_tmp.xiezb_0520").dropDuplicates().cache() 
#从统一绑卡表中选出合适字段并和召回策略用户关联
tp=ss.sql("select * from upw_hive.view_ucbiz_bind_card_inf").select('usr_id','bind_tp','rec_upd_ts')
tp=tp.join(f,tp.usr_id==f.cdhd_usr_id,'left').drop("cdhd_usr_id")
tp=tp.select(tp.rec_upd_ts.substr(6, 2).alias('rec_upd_ts').cast("int"),"usr_id","bind_tp","time","status")
#小于注册月份交易参考月份的卡相加
tp=tp.select (tp.usr_id,tp.time,tp.status,F.when(tp.rec_upd_ts<tp.time,1).otherwise(0).alias('f2'),F.when((tp.rec_upd_ts<tp.time)&(tp.bind_tp=='02'),1).otherwise(0).alias('f1'))
tp=tp.groupby("usr_id","time","status").agg({"f1":"sum","f2":"sum"}).withColumnRenamed("sum(f1)","f3").withColumnRenamed("sum(f2)","f4")
#将当月之前的贷记卡和其所持有的总卡数相除而得
tp=tp.selectExpr("usr_id","time","status","f3/f4 as Credit_card_ratio").cache()
```
tp中就显示了我们想要的结果了，通过tp.show(),这个activation操作就可以快速的知道我们想要的结果。






 
 













