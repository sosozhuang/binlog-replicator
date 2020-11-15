# binlog-replicator
binlog-replicator是一个高性能的[MySQL](https://www.mysql.com) Binlog复制程序。功能可以说很简单，读取MySQL的binlog，并将binlog中的事件和数据复制到[Kafka](https://kafka.apache.org)中。

## 高可用
生产环境下，特别是同步关键数据的场景，必须保证同步数据服务的高可用，任一节点在任何时候都可能存在异常宕机的风险。可以使用部署模式达到高可用：
* MySQL高可用，建议使用MySQL主备半同步复制集群或MGR集群的部署模式。
* binlog-replicator高可用，建议使用基于Zookeeper的主备集群部署模式。
* Kafka高可用，建议使用至少3节点的集群部署模式。

## 传输语义
传输语义使用的是At Least Once，每一个binlog事件复制到Kafka后会检查是否发送成功。碰到异常后会rollback到上一个已经commit的点，然后在重新解析binlog数据并发送到Kafka。存在重复发送数据的情况，需要下游消费者进一步处理。

## 性能
为了达到更高的性能，低并发下降低复制延迟，高并发下提升复制吞吐量，程序实现上主要做了几点优化：
* 使用[Disruptor](https://github.com/LMAX-Exchange/disruptor)作为内存队列，内存预分配，消除伪共享，无锁并发
* 与[Canal Server](https://github.com/alibaba/canal)运行在同一个进程中，无需进行rpc调用和序列化反序列化
* 可自定义线程池参数，不同线程间处理的binlog数据完全无锁
* 根据线程数量创建Kafka生产者，提升网络带宽利用率
* 自定义提升Json序列化性能

## 监控
基于Prometheus的监控，exporter默认运行在11112端口，binlog-replicator添加了一些metrics，主要包括binlog数据转换以及发送的时间以及数量。

## 构建
```bash
git clone https://github.com/sosozhuang/binlog-replicator.git
cd binlog-replicator
mvn clean package
```
生成的tar包在dist目录下。

## 前提
* 安装MySQL 5.7版本，启用binlog row模式并运行
* 安装Zookeeper 3.4.5版本并运行
* 安装Kafka 1.1.1版本并运行

## 运行
```bash
cd dist
tar zxvf binlog-replicator-1.0.0.tar.gz
cd binlog-replicator-1.0.0
./bin/startup.sh
```
执行 `./bin/stop.sh`可以停止程序运行.

## 配置项
|    配置名称    |    类型    |    默认值    |    必输项    |    描述    |
| -------------- | ---------- | ------------ | -----------  |------------- |
| replicator.canal.batchSize | Integer | 1024 | True | 每次向canal获取得binlog数量，控制复制延迟和吞吐 |
| replicator.disruptor.entry.ringBuffer.size | Integer | 2048 | True | 内存队列的长度，必须为2的幂次方 |
| replicator.disruptor.entry.workerThreads  | Integer | CPU个数/2 | True | 内存队列的工作线程数 |
| replicator.disruptor.waitStrategy | Enum | Blocking | True | 内存队列工作线程的等待策略，可为blocking/busy_spin/lite_blocking/sleeping/yielding |
| replicator.kafka.tableTopics | String |    | False | MySQL表对应发送到Kafka topic，格式为"shema.table:topic,schema.table:topic"，表名称可使用正则表达式 |
| replicator.kafka.defaultTopic | String | binlog-replicator | False | replicator.kafka.tableTopics为空或匹配不上时，发送到Kafka的默认topic |
| replicator.kafka.producer.* | Object  |    |    | Kafka生产者配置项，必须配置bootstrap servers |
具体可以参考[replicator.properties](https://github.com/sosozhuang/binlog-replicator/blob/main/src/main/resources/replicator.properties)。
