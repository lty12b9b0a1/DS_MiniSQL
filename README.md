# DS_MiniSQL
Homework for distribution system courses.

问题与细节：

节点上线顺序： curator_server->region_server->client

curator_server双线程：	一个线程为flask服务，监听client请求，regionsever上线等。
				一个线程为定时心跳，更新节点信息。若没有主节点或主节点挂了，选举新主节点。

region_server主节点写入操作：暂定为   client写入->主节点给所有region上锁->主节点开始向所有region广播写入请求->一个region写入完成，该region解锁
					问题：写入顺序性，若有region写入失败等。


- [x] reigon_server上线时的文件同步问题。


- [x] http通信的传入值和返回值的格式问题：eval函数的使用。

- [x] http通信的阻塞模式和异步模式

## 四、MiniSQL系统测试

### 4.1 创建表

``` mysql
create table people (
    ID char(20),
    name char(20),
    age int,
    height float,
    primary key (ID)
);
```


### 4.2 插入记录

``` mysql
insert into people values (
    3150659278,
    'dong',
    20,
    177.5
);

insert into people values (
    3191115278,
    'han',
    22,
    187.5
);

insert into people values (
    3191118888,
    'chen',
    19,
    187.5
);

insert into people values (
    3179615278,
    'wei',
    18,
    187.5
);
```



### 4.3 选择记录 

``` mysql
select * from people;  # 无条件查找

select * from people  # 有条件查找
    where age > 20;
```


### 4.4 删除记录

``` mysql
delete from people where age > 20;  # 条件删除
delete from people;  # 无条件删除
```


### 4.5 创建索引

``` mysql
create index sid on people (ID);
```


### 4.6 删除索引

``` mysql
drop index sid;
```


### 4.7 删除表

``` mysql
drop table people;
```


### 4.8 执行SQL脚本文件

``` mysql
execfile test/test.sql;
```


### 4.9 退出MiniSQL

``` mysql
quit;
```


``` mysql
exit;
```

