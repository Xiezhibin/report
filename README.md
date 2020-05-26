# AI_spark
专题汇报
## Hadoop+Mapreduce+Hive

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





