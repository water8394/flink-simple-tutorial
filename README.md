# Flink 简易使用教程
Flink 是一款能够同时支持高吞吐/低延迟/高性能的分布式处理框架.

 本系列叫做 <Flink简易使用教程>, 目的是记录自己学习 flink 的过程,并且把使用flink的方方面面介绍给大家.尽量用简单的话把使用方法说清楚,在使用某个具体功能的时候能够快速的查找到该使用方法.

 本系列的主要例子会从 flink 官方仓库的 example 出发, 通过这些代码来使用 flink 的一些基本操作.
 文字部分借鉴<Flink原理,实战与性能优化>一书,本书对于初学者十分良好,值得推荐!
 




1. [flink使用01-本系列简介](<https://xinze.fun/2019/09/03/flink%E4%BD%BF%E7%94%A801-%E6%9C%AC%E7%B3%BB%E5%88%97%E7%AE%80%E4%BB%8B/>) 

2. [flink使用02-从WordCount开始](<https://xinze.fun/2019/09/03/flink%E4%BD%BF%E7%94%A802-%E4%BB%8EWordCount%E5%BC%80%E5%A7%8B/>) ([Code Link](https://github.com/CheckChe0803/flink-simple-tutorial/blob/master/streaming/src/main/java/wordcount/WordCount.java))

3. [flink使用03-数据输入的几种不同方法](https://xinze.fun/2019/09/04/flink%E4%BD%BF%E7%94%A803-%E6%95%B0%E6%8D%AE%E8%BE%93%E5%85%A5%E7%9A%84%E5%87%A0%E7%A7%8D%E4%B8%8D%E5%90%8C%E6%96%B9%E6%B3%95/) ([Code Link](https://github.com/CheckChe0803/flink-simple-tutorial/tree/master/streaming/src/main/java/dataSource))

4. [flink使用04-flink使用04-几种时间概念和watermark](https://xinze.fun/2019/09/24/flink%E4%BD%BF%E7%94%A804-%E5%87%A0%E7%A7%8D%E6%97%B6%E9%97%B4%E6%A6%82%E5%BF%B5%E5%92%8Cwatermark/) ([Code Link](https://github.com/CheckChe0803/flink-simple-tutorial/tree/master/streaming/src/main/java/timeAndWatermark))

5. [flink使用05-窗口简介和简单的使用](https://xinze.fun/2019/09/25/flink%E4%BD%BF%E7%94%A805-%E7%AA%97%E5%8F%A3%E7%AE%80%E4%BB%8B%E5%92%8C%E7%AE%80%E5%8D%95%E7%9A%84%E4%BD%BF%E7%94%A8/) ([Code Link](https://github.com/CheckChe0803/flink-simple-tutorial/tree/master/streaming/src/main/java/window/assigner))

6. [flink使用06-如何处理窗口内的数据](https://xinze.fun/2019/09/26/flink%E4%BD%BF%E7%94%A806-%E5%A6%82%E4%BD%95%E5%A4%84%E7%90%86%E7%AA%97%E5%8F%A3%E5%86%85%E7%9A%84%E6%95%B0%E6%8D%AE/) ([Code Link](https://github.com/CheckChe0803/flink-simple-tutorial/tree/master/streaming/src/main/java/window/function))

