# README

本次实验编写基于Hadoop的分布式程序，使用MapReduce实现KNN算法。

- 代码文件
KnnMapReduce.java

- 数据集

 - 训练数据集
 训练数据文件应放在 hdfs 的 /data 文件夹中，并在代码中指定文件名（84行）。
 一条训练数据的格式为 `<value1> <value2> ... <model>` ,即以一个空格隔开的数值数据和最后的类别数据。

 - 测试数据集
 测试数据集作为输入，可以指定存放的目录，实验中放在 hdfs 的 /input 文件夹中。
 一条测试数据的格式与测试数据的相似，但没有最后的类别数据。

- 运行
在已配置好Hadoop的环境中，编译代码文件，将生成的.class文件打包为jar包，使用命令`${HADOOP_HOME}/bin/hadoop jar Knn.jar KnnMapReduce /input /output`运行，其中`${HADOOP_HOME}`为Hadoop的安装目录；`Knn.jar`为前面打包的jar的文件名；`/input`为输入数据所在目录，即测试数据集的目录；`/output`为输出文件目录，可指定。

[GitHub](https://github.com/Azure0Sky/Cloud-Computing-Project)