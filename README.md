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
