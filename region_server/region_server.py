from enum import Enum
from glob import glob
import requests
from threading import Lock, Thread, Event


from execption import MiniSQLSyntaxError
from API import *
from buffer_manager import *
import time
import json
from flask import Flask, request, jsonify, Response
from flask_cors import CORS

server_list = []
server_lock_list = []
server_real_lock_list = []
rotation_lock = 0

import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)
CORS(app, resources=r'/*')

MiniSQLType = Enum('MiniSQLType', ('CREATE_TABLE', 'INSERT', 'DROP_TABLE', 'CREATE_INDEX',
                                   'DROP_INDEX', 'SELECT', 'DELETE', 'QUIT', 'EXECFILE', 'CLEAR'))


def judge_type(query):
    query = query.lower().strip(';\n ').split()
    if query[0] == 'insert':
        return MiniSQLType.INSERT
    elif query[0] == 'select':
        return MiniSQLType.SELECT
    elif query[0] == 'delete':
        return MiniSQLType.DELETE
    elif query[0] in ['quit', 'exit']:
        return MiniSQLType.QUIT
    elif query[0] == 'execfile':
        return MiniSQLType.EXECFILE
    elif query[0] == 'create':
        if query[1] == 'table':
            return MiniSQLType.CREATE_TABLE
        elif query[1] == 'index':
            return MiniSQLType.CREATE_INDEX
    elif query[0] == 'drop':
        if query[1] == 'table':
            return MiniSQLType.DROP_TABLE
        elif query[1] == 'index':
            return MiniSQLType.DROP_INDEX
    elif query[0] == 'clear':
        return MiniSQLType.CLEAR

    raise MiniSQLSyntaxError('Error Type')


def interpret(query, buf, exec_file=False):
    query_type = judge_type(query)
    # query = query.strip('; \n')

    if query_type == MiniSQLType.CREATE_TABLE:
        ret = create_table(query)
    elif query_type == MiniSQLType.CREATE_INDEX:
        ret = create_index(query, buf)
    elif query_type == MiniSQLType.INSERT:
        ret = insert(query, buf)
    elif query_type == MiniSQLType.SELECT:
        ret = select(query, buf)
    elif query_type == MiniSQLType.DELETE:
        ret = delete(query, buf)
    elif query_type == MiniSQLType.DROP_INDEX:
        ret = drop_index(query, buf)
    elif query_type == MiniSQLType.DROP_TABLE:
        ret = drop_table(query, buf)
    elif query_type == MiniSQLType.QUIT:
        ret = 0
    elif query_type == MiniSQLType.EXECFILE:
        ret = execfile(query, buf)
    elif query_type == MiniSQLType.CLEAR:
        ret = clear_all(buf)

    return ret


def execfile(query, buf):
    file = query.strip('; \n').split()[-1]
    with open(file, 'r') as f:
        query = ''
        for i, line in enumerate(f.readlines()):
            query += line
            line = line.strip()
            if line and line[-1] == ';':
                try:
                    ret = interpret(query, buf, True)
                    if isinstance(ret, Buffer):
                        buf = Buffer
                    if ret == 0:
                        break
                    elif isinstance(ret, (list, tuple)):
                        print_table(*ret)
                except MiniSQLError as e:
                    print('In line {}: {}'.format(i + 1, e))
                    break
                query = ''


def print_table(records, cols):
    col_width = 15
    n_cols = len(cols)
    bound = '+' + ''.join(['-' for i in range(n_cols * col_width + n_cols - 1)]) + '+'
    content = '|' + ''.join(['{:^15}|' for i in range(n_cols)])
    print(bound)
    print(content.format(*cols))
    if records:
        for r in records:
            print(bound)
            print(content.format(*r))
    print(bound)


def main():
    query = ''
    buf = Buffer()

    while True:
        print('MiniSQL->', end=' ')
        cmd = input()
        query += cmd + ' '
        cmd = cmd.strip()
        if cmd and cmd[-1] == ';':
            # print(query)
            try:
                beg = time.clock()
                ret = interpret(query, buf)
                end = time.clock()

                if ret == 0:
                    break
                elif isinstance(ret, (list, tuple)):
                    print(*ret)
                    print_table(*ret)

            except MiniSQLError as e:
                print(e)
                
                end = beg
            query = ''

            print('use time {}s'.format(end - beg))

    buf.close()

def synchronize(ip_port):
    url = "http://"+ ip_port + "/testfile0"
    ret = requests.get(url)
    with open("./DB/memory/0.block", "wb") as f:
        f.write(ret.content)

    url = "http://"+ ip_port + "/testfile1"
    ret = requests.get(url)
    with open("./DB/memory/header.hd", "wb") as f:
        f.write(ret.content)
        
    url = "http://"+ ip_port + "/testfile2"
    ret = requests.get(url)
    with open("./DB/catalog/table_schema.minisql", "wb") as f:
        f.write(ret.content)

    url = "http://"+ ip_port + "/testfile3"
    ret = requests.get(url)
    with open("./DB/log.txt", "wb") as f:
        f.write(ret.content)
    
    return 1


def register():
    global server_list
    global server_lock_list
    global server_real_lock_list
    formdata = {"ip_port": SELF_IP_PORT}
    try:
        tmpserver_ip_port_list = requests.get("http://"+CURATOR_IP_PORT+"/register", params=formdata)
        new_server_list = json.loads(tmpserver_ip_port_list.content)

        new_server_lock_list = []
        for i in range(len(new_server_list)):
            if new_server_list[i] in server_list:
                new_server_lock_list.append(server_lock_list[server_list.index(new_server_list[i])])
            else:
                new_server_lock_list.append(0)
            newlock = Lock()
            server_real_lock_list.append(newlock)

        server_lock_list = new_server_lock_list
        server_list = new_server_list



        if server_list[0]["address"] != SELF_IP_PORT:
            synchronize(server_list[0]["address"])
        return 1
    except Exception as e:
        return 0
        

def signout():
    formdata = {"ip_port": SELF_IP_PORT}
    try:
        requests.get("http://"+CURATOR_IP_PORT+"/signout", params=formdata)
        print("节点退出")
    except Exception as e:
        print(e)


@app.route('/testfile0', methods=['get'])
def testfile0():
    file = open('./DB/memory/0.block', 'rb')
    raw = file.read()
    return Response(raw)

@app.route('/testfile1', methods=['get'])
def testfile1():
    file = open('./DB/memory/header.hd', 'rb')
    raw = file.read()
    return Response(raw)

@app.route('/testfile2', methods=['get'])
def testfile2():
    file = open('./DB/catalog/table_schema.minisql', 'rb')
    raw = file.read()
    return Response(raw)

@app.route('/testfile3', methods=['get'])
def testfile3():
    file = open('./DB/log.txt', 'rb')
    raw = file.read()
    return Response(raw)


@app.route('/heartbeat', methods=['get'])
def heartbeat():
    global server_list
    global server_lock_list
    global server_real_lock_list
    tmpserver_list = request.args.get('server_list')
    # print(server_list)
    # print(json.loads(server_list)[0])
    new_server_list = json.loads(tmpserver_list)
    new_server_lock_list = []
    new_server_real_lock_list = []
    for i in range(len(new_server_list)):
        if new_server_list[i] in server_list:
            new_server_lock_list.append(server_lock_list[server_list.index(new_server_list[i])])
            new_server_real_lock_list.append(server_real_lock_list[server_list.index(new_server_list[i])])
        else:
            new_server_lock_list.append(0)
            newlock = Lock()
            new_server_real_lock_list.append(newlock)
    server_lock_list = new_server_lock_list
    server_list = new_server_list
    server_real_lock_list = new_server_real_lock_list

    print("~pulse~ 当前的节点列表：", server_list)
    return json.dumps(1)

@app.route('/selectregion', methods=['get'])
def selectregion():
    global rotation_lock
    orgin_lock = rotation_lock
    while 1:
        rotation_lock = rotation_lock + 1
        if rotation_lock >= len(server_list):
            rotation_lock = 0
        if server_lock_list[rotation_lock] == 0:
            return json.dumps(server_list[rotation_lock])
        if rotation_lock == orgin_lock:
            break

    return json.dumps("error: no region availible!")


def ismaster():
    global server_list
    for i in server_list:
        if i["address"] == SELF_IP_PORT:
            if i["isMaster"] == True:
                return 1
            else:
                return 0


# 向所有节点广播写入信息
class WqueryThread(Thread):
    def __init__(self, id, query, address, lock, success, failed):
        super(WqueryThread, self).__init__()
        self.id = id
        self.address = address
        self.query = query
        self.lock = lock
        self.success = success
        self.failed = failed
    def run(self):
        global server_list
        global server_lock_list
        global server_real_lock_list

        server_lock_list[i] = 1
        server_real_lock_list[self.id].acquire()
        try:
            url = "http://" + self.address + "/query_broadcast"
            response = requests.get(url=url, params=self.query, timeout=1)
            # 返回值为数字即视为存活
            if response.content == 1:
                self.success.append(self.id)
            else:
                self.failed.append(self.id)

        except Exception as e:
            print(e)
            self.failed.append(self.id)
        
        if len(self.success) >= W_SUCCESS_THRESHOLD or len(self.failed) >= len(server_list) - W_SUCCESS_THRESHOLD:
            self.lock.set()
        
        self.lock.wait()
        
        if response.content == 1 and len(self.success) < W_SUCCESS_THRESHOLD:
            synchronize(server_list[self.failed[0]]["address"])
        if response.content == 0 and len(self.success) >= W_SUCCESS_THRESHOLD:
            synchronize(server_list[self.success[0]]["address"])
        
        server_lock_list[self.id] = 0
        server_real_lock_list[self.id].release()


write_lock = Lock()

@app.route('/wquery', methods=['get'])
def wquery():
    finish = Event()
    success_num = []
    failed_num = []
    global write_lock
    if ismaster() == 0:
        return jsonify("master changed!")
    query = request.args.get('query')
    print(query)
    global server_list
    global server_lock_list
    write_lock.acquire()

    finish.clear()
    for i in range(len(server_list)):
        pulse_thread = WqueryThread(server_list[i]["address"], query, finish, success_num, failed_num)
        pulse_thread.start()
    
    finish.wait()
    write_lock.release()
    if len(success_num) >= W_SUCCESS_THRESHOLD:
        return "1"
    else:
        return "0"


@app.route('/query_broadcast')
def query_broadcast():
    query = request.args.get('query')
    print(query)
    try:
        beg = time.clock()
        ret = interpret(query, buf)
        end = time.clock()
        print(ret)
    except MiniSQLError as e:
        return jsonify(e.args)
    query = ''

    # print('use time {}s'.format(end - beg))
    return jsonify(ret)

@app.route('/testsleep')
def testsleep():
    x = 0
    while 1:
        x = x + 1
        if x >= 1000000000:
            break
    
    return 0


if __name__ == '__main__':
    # query = ''
    try:
        if register() == 1:
            buf = Buffer()
            app.run(host="0.0.0.0", port=SELF_PORT)
            buf.close()
            signout()
        else:
            print("register failed!")
    except Exception as e:
        print(e)
    # main()
