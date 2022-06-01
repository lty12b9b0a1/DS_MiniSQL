import json
import time
from threading import Lock, Thread
import requests
from flask import Flask, request
from flask_cors import CORS
from pynput import keyboard

# Curator维护一个servers.json文件，存放所有节点的地址和是否为主节点信息，例如[{"address": "127.0.0.1:5001", "isMaster": false}]
# 每10秒一次心跳检测，按下Esc键退出curator程序

import logging

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)
CORS(app, supports_credentials=True)
new_servers = []
table_region_list = []
index_region_list = []
current_master = []
current_leader = None
write_lock = Lock()


@app.route("/")
def hello_world():
    print(request.remote_addr)
    return "<p>hello,world!<p>"


@app.route("/getLeader")
def get_Leader():
    return json.dumps(current_leader)


# 通过此接口获取主节点，格式为"ip:port"
@app.route("/getMaster")
def get_master():
    return json.dumps(current_master)


# 通过此接口获取所有节点列表，格式为json
@app.route("/getServer")
def get_server():
    with open("servers.json", "r") as json_file:
        return json.dumps(json.load(json_file))


# 节点登出
@app.route("/signout")
def signout():
    global write_lock
    global current_master
    global current_leader
    servers = []
    with open("servers.json", "r") as json_file:
        servers = json.load(json_file)
        address = request.args.get("ip_port")
        region = request.args.get("region")
        if {"address": address, "isMaster": False, "isLeader": False, "region": region} in servers:
            del servers[servers.index({"address": address, "isMaster": False, "isLeader": False, "region": region})]
        if {"address": address, "isMaster": True, "isLeader": False, "region": region} in servers:
            del servers[servers.index({"address": address, "isMaster": True, "isLeader": False, "region": region})]
            for i in range(len(current_master)):
                if current_master[i]["region"] == region:
                    current_master[i]["master"] = None
                    break
            # current_master = None
        if {"address": address, "isMaster": True, "isLeader": True, "region": region} in servers:
            del servers[servers.index({"address": address, "isMaster": True, "isLeader": True, "region": region})]
            for i in range(len(current_master)):
                if current_master[i]["region"] == region:
                    current_master[i]["master"] = None
                    break
            # current_master = None
            current_leader = None
            
    write_lock.acquire()
    with open("servers.json", "w") as json_file:
        json.dump(servers, json_file)
    write_lock.release()
    print("节点登出:", address, "region:", region)
    return json.dumps(servers)


# 通过此接口向Curator上线新的节点，调用时需要传递参数
@app.route("/register")
def server_online():
    global write_lock
    global current_master
    with open("servers.json", "r") as json_file:
        servers = json.load(json_file)
        address = request.args.get("ip_port")
        region = request.args.get("region")
        servers.append({"address": address, "isMaster": False, "isLeader": False, "region": region})
    write_lock.acquire()
    with open("servers.json", "w") as json_file:
        json.dump(servers, json_file)
    write_lock.release()


    tmpfind = 0
    
    for i in range(len(current_master)):
        if current_master[i]["region"] == region:
            tmpfind = 1
            break
    
    if tmpfind == 0:
        current_master.append({"master": None, "region": region})


    print("新节点注册:", address, "region:", region)
    return json.dumps(servers)


# 向对应节点发送请求验证其是否存活
class PulseThread(Thread):
    def __init__(self, is_master, is_leader, address, region, lock):
        super(PulseThread, self).__init__()
        self.is_master = is_master
        self.address = address
        self.lock = lock
        self.is_leader = is_leader
        self.region = region

    def run(self):
        global new_servers
        global current_master
        global current_leader
        global table_region_list
        global index_region_list
        try:
            url = "http://" + self.address + "/heartbeat"
            with open("servers.json", "r") as json_file:
                # 注意此处传递字典的值为字典列表，需要序列化
                servers = {"server_list": json.dumps(json.load(json_file)), "table_region_list": json.dumps(table_region_list), "index_region_list": json.dumps(index_region_list)}
            response = requests.get(url=url, params=servers, timeout=2)
            # 返回值为数字即视为存活
            if response.content.decode("utf-8").isdigit():
                1
            else:
                print(response.content)
                tmprs = json.loads(response.content)
                table_region_list = tmprs["tablelist"]
                index_region_list = tmprs["indexlist"]
            new_server = {"address": self.address, "isMaster": self.is_master, "isLeader": self.is_leader, "region": self.region}
            self.lock.acquire()
            new_servers.append(new_server)
            self.lock.release()
        except Exception as e:
            print(e)
            if self.is_master:
                for i in range(len(current_master)):
                    if current_master[i]["region"] == self.region:
                        current_master[i]["master"] = None
                        break
                # current_master = None
            if self.is_leader:
                current_leader = None


# 向所有记录中的节点发送请求等待响应，每个节点启动一个PulseThread线程
def pulse():
    global new_servers
    global current_master
    global current_leader

    global write_lock
    new_servers = []
    thread_list = []
    with open("servers.json", "r") as json_file:
        servers = json.load(json_file)
        lock = Lock()
        for server in servers:
            is_master = server["isMaster"]
            is_leader = server["isLeader"]
            address = server["address"]
            region = server["region"]
            pulse_thread = PulseThread(is_master, is_leader, address, region, lock)
            pulse_thread.start()
            thread_list.append(pulse_thread)
        for thread in thread_list:
            thread.join()
        # 当前无主节点时选择新的主节点：new_servers列表中的第一个节点
        for i in range(len(current_master)):           
            if current_master[i]["master"] == None and len(new_servers) > 0:
                for server in new_servers:
                    if server["region"] == current_master[i]["region"]:
                        server["isMaster"] = True
                        current_master[i]["master"] = server["address"]
                        break
        if current_leader is None and len(new_servers) > 0:
            for server in new_servers:
                if server["isMaster"] == True: 
                    server["isLeader"] = True
                    current_leader = server["address"]
                    break

        print("当前的节点列表：" + str(new_servers))
    write_lock.acquire()
    with open("servers.json", "w") as json_file:
        json.dump(new_servers, json_file)
    write_lock.release()


# CuratorApp的运行线程
class AppThread(Thread):
    def __init__(self):
        super().__init__()

    def run(self):
        app.run(host="0.0.0.0", port=5000)


# 心跳检测线程
class HeartThread(Thread):
    def __init__(self):
        super().__init__()

    def run(self):
        time.sleep(1)
        while True:
            pulse()
            time.sleep(3)


def clear_server():
    servers = []
    with open("servers.json", "w") as json_file:
        json.dump(servers, json_file)


def on_press(key):
    if key == keyboard.Key.esc:
        return False


if __name__ == "__main__":
    print("Curator开始运行，按下Esc键以退出！")
    print("------------------------------------------------------------")
    app_thread = AppThread()
    app_thread.setDaemon(True)
    app_thread.start()
    heart_thread = HeartThread()
    heart_thread.setDaemon(True)
    heart_thread.start()
    while True:
        with keyboard.Listener(on_press=on_press) as listener:
            listener.join()
            break
    clear_server()
    print("Curator已安全退出！")
