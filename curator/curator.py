import json
import time
from threading import Lock, Thread
import requests
from flask import Flask, request
from flask_cors import CORS
from pynput import keyboard

# Curator维护一个servers.json文件，存放所有节点的地址和是否为主节点信息，例如[{"address": "127.0.0.1:5001", "isMaster": false}]
# 每10秒一次心跳检测，按下Esc键退出curator程序

app = Flask(__name__)
CORS(app, supports_credentials=True)
new_servers = []
current_master = None
write_lock = Lock()


@app.route("/")
def hello_world():
    print(request.remote_addr)
    return "<p>hello,world!<p>"


# 通过此接口获取主节点，格式为"ip:port"
@app.route("/getMaster")
def get_master():
    return json.dumps(current_master)


# 通过此接口获取所有节点列表，格式为json
@app.route("/getServer")
def get_server():
    with open("servers.json", "r") as json_file:
        return json.dumps(json.load(json_file))


# 通过此接口向Curator上线新的节点，调用时需要传递参数
@app.route("/register")
def server_online():
    global write_lock
    with open("servers.json", "r") as json_file:
        servers = json.load(json_file)
        address = request.args.get("ip_port")
        servers.append({"address": address, "isMaster": False})
    write_lock.acquire()
    with open("servers.json", "w") as json_file:
        json.dump(servers, json_file)
    write_lock.release()
    return json.dumps(servers)


# 向对应节点发送请求验证其是否存活
class PulseThread(Thread):
    def __init__(self, is_master, address, lock):
        super(PulseThread, self).__init__()
        self.is_master = is_master
        self.address = address
        self.lock = lock

    def run(self):
        global new_servers
        global current_master
        try:
            url = "http://" + self.address + "/heartbeat"
            with open("servers.json", "r") as json_file:
                # 注意此处传递字典的值为字典列表，需要序列化
                servers = {"server_list": json.dumps(json.load(json_file))}
            response = requests.get(url=url, params=servers, timeout=5)
            # 返回值为数字即视为存活
            if response.content.decode("utf-8").isdigit():
                new_server = {"address": self.address, "isMaster": self.is_master}
                self.lock.acquire()
                new_servers.append(new_server)
                self.lock.release()
        except Exception as e:
            print(e)
            if self.is_master:
                current_master = None


# 向所有记录中的节点发送请求等待响应，每个节点启动一个PulseThread线程
def pulse():
    global new_servers
    global current_master
    global write_lock
    new_servers = []
    with open("servers.json", "r") as json_file:
        servers = json.load(json_file)
        lock = Lock()
        for server in servers:
            is_master = server["isMaster"]
            address = server["address"]
            pulse_thread = PulseThread(is_master, address, lock)
            pulse_thread.start()
            pulse_thread.join()
        # 当前无主节点时选择新的主节点：new_servers列表中的第一个节点
        if current_master is None and len(new_servers) > 0:
            for server in new_servers:
                server["isMaster"] = True
                current_master = server["address"]
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
        app.run(host="127.0.0.1", port=5000)


# 心跳检测线程
class HeartThread(Thread):
    def __init__(self):
        super().__init__()

    def run(self):
        time.sleep(1)
        while True:
            pulse()
            time.sleep(10)


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
    print("Curator已安全退出！")
