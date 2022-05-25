# DS_MiniSQL
Homework for distribution system courses.

python version == 3.6.13

build up(with conda)：

1. create a new conda environment.

   $conda create -n your_env_name

2. activate new env

   $conda activate your_env_name

3. install the requirements:

   $conda install pip (if pip is not installed before)

   $pip install -r requiremnts

4. launch the servers.

   4.1 launch curator server:

   ​	$ cd ./curator/ 

   ​	$ python curator.py

   4.2 lauch region server:

   ​	$ cd ./region_server

   ​	$ python region_server.py

   4.3 lauch client:

   ​	$ cd ./client

   ​	$ python client_master.py



build up(without conda)：

 1. install the requirements:

    $ pip install -r requirements

 2. launch the servers.

(To test the system effficiently, please launch at least one region server. If the test is done locally, please change the port in the ./region_server/config.py)



## MiniSQL系统测试

### 创建表

``` mysql
create table people (
    ID char(20),
    name char(20),
    age int,
    height float,
    primary key (ID)
);
```


### 插入记录

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



### 选择记录 

``` mysql
select * from people;  # 无条件查找

select * from people  # 有条件查找
    where age > 20;
```


### 删除记录

``` mysql
delete from people where age > 20;  # 条件删除
delete from people;  # 无条件删除
```


### 创建索引

``` mysql
create index sid on people (ID);
```


### 删除索引

``` mysql
drop index sid;
```


### 删除表

``` mysql
drop table people;
```


### 退出MiniSQL


``` mysql
exit;
```

