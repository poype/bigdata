BigTable论文笔记



目标一：支撑每秒十万、乃至百万级别的**随机**读写请求。

目标二：解决好“可伸缩性”和“可运维性”的问题。在一个上千台服务器的集群上，Bigtable如何做到自动分片和灾难恢复。



Bigtable 在一开始，也不准备先考虑事务、Join 等高级的功能，而是把核心放在了“可伸缩性”上。

## Bigtable的基本数据模型

Bigtable的数据模型是一个很宽的**稀疏**表，每一行就是一条数据。

##### 1. 主键

每条数据里都有一个行键(Row Key)，行键就是每条数据的主键。Bigtable支持**通过主键随机读写**表中的每条记录。因为只能通过主键来读写数据，所以这种数据库也叫做**KV 数据库**。

##### 2. 列族Column Family

Bigtable的表中包含一个或多个列族(Column Family)。在每个Column Family下，不需要提前指定包含哪些列(Column)。每一行数据都可以有属于自己专有的列，每一行数据的列也可以完全不一样，各个行中的列不是固定的。

##### 3. 稀疏

Bigtable在底层存储数据的时候，会将每一行记录的**列和值**都存储下来。一行中没有某个值，就意味着该行中没有那个列。这也是为什么说Bigtable是一个**“稀疏”**的表。

##### 4. 多版本

一行记录在某个列下面如果有值的话，可以存储多个版本的值。不同版本会对应不同的时间戳(Timestamp)，可以指定保留最近的N个版本(比如N=3，就是保留时间戳最近的三个版本)，也可以指定保留某一个时间点之后的版本。

结合上面的四点，BigTable的数据模型结构如下图所示：

![image-20240122115250531](D:\Workspace\bigdata\hbase_doc\img\image-20240122115250531.png)

##### 5. 物理存储

**一行记录中属于同一个列族下的数据会在物理上存储在一起**。Bigtable的开源实现HBase，就是把每一个列族的数据存储在同一个HFile文件里。

下面是HBase的数据模型示意图，可以看到即使两个不同的Column Family属于同一行，但不同的Column Family数据在底层存储是放在不同的Region中的。

![image-20240122115805596](D:\Workspace\bigdata\hbase_doc\img\image-20240122115805596.png)

每行记录中的列和值，都是直接以**key-value**键值对的形式直接存储下来的。

一行HBase的数据其实是**分为好几行**存储的，一个列对应一行，HBase的key-value结构如下：

![image-20240122121234575](D:\Workspace\bigdata\hbase_doc\img\image-20240122121234575.png)

可以看到Row11这条记录中phone这列是没有数据的，在HBase的底层存储里面也就没有存储这列。其次也能看出不同Column Family的数据是存储在不同的文件中的。

下图展示了当我们修改HBase表的某一列时，HBase是如何存储的：

![image-20240122121850176](D:\Workspace\bigdata\hbase_doc\img\image-20240122121850176.png)

将Row1的city列从北京修改为上海，可以看到在底层其实存储了两条数据，这两条数据的版本是不一样的，最新的一条数据版本比之前的新。这也就是为什么Hbase能够很容易的为一行中的某个列存储**多个版本数据**的原因。

1. HBase支持数据多版本特性，通过带有不同时间戳的多个KeyValue版本来实现的；
2. 每次put，delete都会产生一个新的Cell，都拥有一个版本；
3. 默认只存放数据的三个版本，可以配置；
4. 查询默认返回最新版本的数据，可以通过指定版本号获取旧数据。

Bigtable的这个数据模型，使得我们能很容易地在表中增加列，而且增加列并不算修改Bigtable表的Schema。如果一行记录需要有某个列的值，直接写入数据就好了。这带来了很大的灵活性，特别适合早期的互联网业务。

## 数据分区

数据分区是指把一个数据表，根据主键的不同，拆分到多个不同的服务器上。分区之后的每一片数据，在不同的分布式系统里有不同的名字，在MySQL里一般叫做Shard，Bigtable里则叫做Tablet，HBase中叫做Region。

##### MySQL分库分表的缺点

MySQL分库分表通常采用哈希分区。我们会拿一个字段哈希取模，然后划分到预先定好N个分片里面。

这里最大的问题，在于分区需要在一开始就设计好，而不是自动随着数据变化动态调整的。往往计划不如变化快，当业务变化和计划稍有不同，就会遇到各个分片负载不均衡、扩缩容需要搬运大量数据等情况。例如如果将4台MySQL服务器扩展到6台的时候，哈希分区的方式使得我们要在网络上搬运整个数据库2/3的数据。

##### Bigtable动态区间分区

Bigtable采用了动态区间分区方式。Bigtable不是一开始就定义好需要多少个机器，应该如何分区，而是采用了一种“自动分裂”的方式来动态地进行分区。

在Bigtable数据表中，所有行记录会按照主键排好序，然后按照连续的主键一段一段地分区。

在某一段主键的区间里，如果写的数据越来越多，占用的存储空间越来越大，那么整个系统会自动地将这个分区一分为二，变成两个分区。而如果某一个主键区间段的数据被删掉了很多，占用的空间越来越小，那么Bigtable就会自动把这个分区和它旁边的分区合并到一起。

这个分区的过程，就好像你按照A~Z的字母顺序去管理书的过程。一开始，只有一个空箱子，然后你把书按照书名的拼音排好序放到箱子里。当有一本新书需要放进来的时候，你就**按照书名的字母顺序插在某两本书中间(始终保持行记录按主键有序)**。当某个箱子放不下书的时候，你就再拿一个空箱子，放在放不下的箱子下面，然后把前一个箱子里的图书从中间一分，把下面的一半放到新箱子里。而把书从箱子里面拿走时，如果两个相邻的箱子里都很空，我们就可以把两个箱子里面的书放到一个箱子里面，然后把腾出来的空箱子挪走。这里的一个个“箱子”就对应分片，这里面的一本本书就对应每一行数据，而书名的拼音就对应行记录中的主键。可能以 A、B、C 开头的书多一些，那么它们占用的分区就会多一些，以 U、V、W 开头的书少一些，可能这些书就都在一个分区里面。

采用这样的方式，每个分区在数据量上都会相对比较均匀。而且，在分区发生变化的时候，你需要调整的只有一个分区，而没有大量搬运数据的压力。

依靠动态分区机制，Bigtable具有非常好的**可伸缩性**和**可运维性**。根据存储数据量的大小，可以很容易地将集群扩容到成百上千个节点，并能在一个上千台服务器的集群上做到自动分片和灾难恢复。这就需要Bigtable有一套存储和管理分区信息的机制。

## Bigtable架构

**Master、Chubby、Tablet Server、GFS**四个组件共同构成了整个Bigtable集群。Bigtable的架构图如下所示：

![image-20240122151254381](D:\Workspace\bigdata\hbase_doc\img\image-20240122151254381.png)

Bigtable的开源实现HBase的架构图如下所示：

![image-20240122154133228](D:\Workspace\bigdata\hbase_doc\img\image-20240122154133228.png)

##### Tablet Server

Tablet Server的角色最明确，它就是用来实际提供数据读写服务的。一个Tablet Server上会分配到10到1000个Tablets，Tablet Server 就负责这些Tablets的读写请求，并且当单个Tablet太大时，对其进行分裂。

##### 在线服务与存储分离

Bigtable的Tablet Server只负责在线服务，不负责数据存储。实际的存储，是通过一种叫做SSTable的数据格式写入到GFS上的。也就是 Bigtable里，数据存储和在线服务的职责是完全分离的。我们调度Tablet的时候，只是调度在线服务的负载，并不需要把数据也一并搬运走。

##### Master

哪些Tablets分配给哪个Tablet Server，是由Master负责的。Master可以根据每个Tablet Server的负载进行动态的调度。

Master一共会负责5项工作：

1. 分配Tablets给Tablet Server；
2. 检测Tablet Server的新增和过期；
3. 平衡Tablet Server的负载；
4. 对于GFS上的数据进行垃圾回收；
5. 管理表和列族的Schema变更，比如表和列族的创建与删除。

##### Chubby的作用

Bigtable需要Chubby来搞定这么几件事儿：

1. 确保我们只有一个Master；
2. 存储Bigtable 数据的引导位置(Bootstrap Location)；
3. 发现Tablet Servers以及在它们终止之后完成清理工作；
4. 存储Bigtable的Schema信息；
5. 存储 ACL，也就是Bigtable的访问权限。

##### METADATA表存储分区信息

Bigtable把分区信息存到了METADATA表中。METADATA表就是一张Bigtable的数据表，只是这张表存储的是Bigtable各个tablet的分区信息。

这其实有点像MySQL里面的information_schema表，也就是数据库定义了一张特殊的表，用来存放自己的元数据。不过，Bigtable是一个分布式数据库，所以我们需要知道METADATA表中的数据究竟存放在哪个Tablet Server里，这个就需要通过Chubby来告诉我们了。

1. Bigtable在Chubby里的一个指定的文件中，存放了Root Tablet分区所在的位置。Root Tablet分区是METADATA表的第一个分区，这个分区永远不会分裂。它里面存的是METADATA表里其他Tablets所在的Tablet Server。
2. 而METADATA中剩下的那些Tablet分区，每一个Tablet中都存放了用户创建的那些数据表所包含的Tablet分区所在的位置，也就是所谓的User Tablet所在的Tablet Server。



![image-20240122164948739](D:\Workspace\bigdata\hbase_doc\img\image-20240122164948739.png)

METADATA表的一条记录大约是1KB，假设METADATA的Tablet如果限制在128MB，则Root Tablet可以包含`1024 * 128 = 131072`个Tablet分区映射关系，则整个Bigtable集群可以包含的用户表Tablet的数量可以高达`1024 * 128 * 1024 * 128 = 171亿`个，一定够用了。

例如Bigtable有一张叫做ECOMMERCE_ORDERS的订单表，通过订单编号A20210101RST查询订单数据的过程如下：

1. 客户端先去查询Chubby，看Root Tablet在哪里。Chubby会告诉客户端，Root Tablet在5号Tablet Server。
2. 客户端会再向Server TS5发起请求，从Root Tablet中查询METADATA表中的哪个Tablet存放了ECOMMERCE_ORDERS业务表中主键为A20210101RST的记录的**位置**。Server TS5告诉客户端，这个信息可以在Server TS8上面的METADATA表中的tablet 107上查询到。
3. 客户端再发起请求到Server TS8，从METADATA表的tablet 107查询ECOMMERCE_ORDERS业务表中主键为A20210101RST的位置信息。Server TS8告诉客户端，这个数据在Server TS20的tablet 253里面。
4. 客户端发起最后一次请求给Server TS20，从tablet 253中查询ECOMMERCE_ORDERS业务表中主键为A20210101RST的具体数据，Server TS20最终会把数据返回给客户端。

查询过程示意图如下：

![image-20240122182933985](D:\Workspace\bigdata\hbase_doc\img\image-20240122182933985.png)

可以看到，一次Bigtable的查询过程会有3次网络请求用于找到行记录所在的位置，再通过1次网络请求获取到行记录具体的数据内容。

一般我们会把前三次查询位置的结果缓存在客户端，以减少往返网络的查询次数。而对于整个METADATA表来说，Tablet Server会把METADATA表中的数据都保留在内存里，这样每个Bigtable请求都要读写的数据，就不需要通过访问GFS来获取到了。

这个设计带来了一个很大的好处，就是查询一条数据所在Tablet的位置信息这件事情，尽可能地被分摊到了Bigtable的整个集群(**因为METADATA表的各个Tablet是分散在集群中的各个Server上**)，而不是集中在某一个Master节点上。而唯一所有人都需要查询的Root Tablet的位置和里面的数据，考虑到Root Tablet不会分裂，并且客户端可以有缓存，Chubby和 Root Tablet所在的Server也不会有太大压力。

##### Master调度

单纯的数据读写过程不需要Master参与。Master只负责Tablet的调度而已，而且这个调度功能，也是依赖Chubby完成的。

1. 所有的Tablet Server一旦上线，就会在Chubby下的一个指定目录下获得一个和自己名字相同的独占锁（exclusive lock）。可以看作是，Tablet Server 把自己注册到集群上了。
2. Master会一直监听这个目录，当发现一个Tablet Server注册了，它就知道有一个新的Tablet Server加入了集群，可以向这个新的Server分配Tablet。分配Tablet的情况很多，可能是因为其他的Tablet Server挂了，导致部分Tablet没有分配出去，或者因为别的Tablet Server的负载太大，这些情况都可以让Master去重新分配Tablet。
3. Tablet Server本身，是根据是否还独占着Chubby上对应的锁以及锁文件是否还在，来确定自己是否还为自己分配到的Tablet服务。比如Tablet Server到Chubby的网络中断了，那么Tablet Server就会失去这个独占锁，也就不再为原先分配到的Tablet提供服务了。而如果我们把Tablet Server从集群中挪走，那么Tablet Server会主动释放锁，当然它也不再服务那些Tablet了，这些 Tablets 都需要重新分配。
4. 无论是异常情况，或是正常的情况，都是由Master来检测Tablet Server是不是正常工作的。检测的方法也不复杂，其实就是通过心跳。Master会定期问Tablet Server，你是不是还占着独占锁呀？无论是Tablet Server说它不再占有锁了，还是 Master连不上Tablet Server了，Master 都会做一个小小的测试，就是自己去获取这个锁。如果 Master 能够拿到这个锁，说明 Chubby 还活得好好的，那么一定是 Tablet Server 那边出了问题，Master 就会删除这个锁，确保Tablet Server不会再为对应的Tablet提供服务。而原先 Tablet Server上的Tablets 就会变回一个未分配的状态，需要回到上面的第2点重新分配。
5. 而Master自己，一旦和Chubby之间的网络连接出现问题，也就是它和Chubby之间的会话过期了，它就会选择“自杀”，这个是为了避免出现两个Master而不自知的情况。反正，Master的存活与否，不影响已经存在的Tablets分配关系，也不会影响到整个Bigtable数据读写的过程。