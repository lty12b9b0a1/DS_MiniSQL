from enum import Enum
import requests
from threading import Lock, Thread, Event
from toolkit.compress import compress_file
from toolkit.decompress import decompress

from execption import MiniSQLSyntaxError
from API import *
from buffer_manager import *
import time
import json
from flask import Flask, request, jsonify, Response
from flask_cors import CORS


region_load_timer = 0
region_load_list = []
table_region_list = []
index_region_list = []

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


@app.route('/synchronize_q', methods=['get'])
def synchronize_q():
    ip_port = request.args.get('ip_port')
    synchronize(ip_port)

def synchronize(ip_port):
    url = "http://"+ ip_port + "/file_ts"
    ret = requests.get(url)
    print("register sync with:", ip_port)
    with open("./tmp.zip", "wb") as f:
        f.write(ret.content)
    
    decompress("./tmp.zip", "./")
    

    return 1


def register():
    global server_list
    global server_lock_list
    global server_real_lock_list
    formdata = {"ip_port": SELF_IP_PORT, "region": SELF_REGION}
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


        for i in range(len(server_list)):
            if server_list[i]["address"] != SELF_IP_PORT and server_list[i]["region"] == SELF_REGION:
                synchronize(server_list[i]["address"])
                break

        return 1
    except Exception as e:
        return 0
        

def signout():
    formdata = {"ip_port": SELF_IP_PORT, "region": SELF_REGION}
    try:
        requests.get("http://"+CURATOR_IP_PORT+"/signout", params=formdata)
        print("节点退出")
    except Exception as e:
        print(e)


@app.route('/file_ts', methods=['get'])
def file_ts():
    global buf
    buf.close()
    compress_file("./tmp.zip", "./DB")
    file = open('./tmp.zip', 'rb')
    raw = file.read()
    buf = Buffer()
    return Response(raw)


@app.route('/heartbeat', methods=['get'])
def heartbeat():
    global region_load_timer
    global region_load_list
    global table_region_list
    global index_region_list
    print(table_region_list)
    print(region_load_list)
    region_load_timer = region_load_timer + 1
    if region_load_timer >= 100:
        region_load_timer = 0
        for i in range(len(region_load_list)):
            region_load_list[i]["load"] = 0

    global server_list
    global server_lock_list
    global server_real_lock_list
    tmpserver_list = request.args.get('server_list')
    # print(server_list)
    # print(json.loads(server_list)[0])
    new_server_list = json.loads(tmpserver_list)

    tmptable_region_list = request.args.get('table_region_list')
    # print(server_list)
    # print(json.loads(server_list)[0])
    new_table_region_list = json.loads(tmptable_region_list)

    tmpindex_region_list = request.args.get('index_region_list')
    # print(server_list)
    # print(json.loads(server_list)[0])
    new_index_region_list = json.loads(tmpindex_region_list)

    if isleader() == 0:
        table_region_list = new_table_region_list
        index_region_list = new_index_region_list
    else:
        for i in range(len(new_server_list)):
            tmpfind = 0
            for j in range(len(region_load_list)):
                if region_load_list[j]["region"] == new_server_list[i]["region"]:
                    tmpfind = 1
                    break
            if tmpfind == 0:
                region_load_list.append({"region": new_server_list[i]["region"], "load": 0})
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
    if isleader:
        return json.dumps({"tablelist": table_region_list, "indexlist": index_region_list})
    else:
        return json.dumps(1)

def get_index_name(query):
    query_type = judge_type(query)
    if query_type == MiniSQLType.CREATE_INDEX:
        query = query.strip('; \n')
        match = re.match(r'^create\s+index\s+(.+)\s+on\s+(.+)\s*\((.+)\)$', query, re.S)
        if match:
            return match.group(1).strip()
        else:
            return ""
    elif query_type == MiniSQLType.DROP_INDEX:
        query = query.strip('; \n')
        match = re.match(r'^drop\s+index\s+(.+)$', query, re.S)
        if match:
            return match.group(1).strip()
        else:
            return ""



def get_table_name(query):
    query_type = judge_type(query)
    if query_type == MiniSQLType.CREATE_TABLE:
        query = query.strip('; \n')
        match = re.match(r'^create\s+table\s+([a-z][a-z0-9_]*)\s+\((.+)\)$', query, re.S)
        if match:
            table, attribs = match.groups()
            return table.strip()
        else:
            return ""
    elif query_type == MiniSQLType.CREATE_INDEX:
        query = query.strip('; \n')
        match = re.match(r'^create\s+index\s+(.+)\s+on\s+(.+)\s*\((.+)\)$', query, re.S)
        if match:
            return match.group(2).strip()
        else:
            return ""
    elif query_type == MiniSQLType.INSERT:
        query = query.strip('; \n')
        match = re.match(r'^insert\s+into\s+(.+)\s+values\s*\((.+)\)$', query, re.S)
        if match:
            return match.group(1).strip()
        else:
            return ""
    elif query_type == MiniSQLType.SELECT:
        query = query.strip('; \n')
        match = re.match(r'^select\s+(.+)\s+from\s+(.*)\s+where\s+(.+)$', query, re.S)
        if match:
            print("match!!")
            cols, table, condition = match.groups()
            return table.strip()
        else:
            match = re.match(r'^select\s+(.*)\s+from\s+(.*)$', query, re.S)
            if match:
                cols, table = match.groups()
                return table.strip()
            else:
                return ""

    elif query_type == MiniSQLType.DELETE:
        query = query.strip('; \n')
        match = re.match(r'^delete\s+from\s+(.+)\s+where\s+(.+)$', query, re.S)
        if match:
            table, condition = match.groups()
            return table.strip()
        else:
            match = re.match(r'^delete\s+from\s+(.+)', query, re.S)
            if match:
                table = match.group(1)
                return table.strip()
            else:
                return ""
    elif query_type == MiniSQLType.DROP_INDEX:
        query = query.strip('; \n')
        match = re.match(r'^drop\s+index\s+(.+)$', query, re.S)
        if match:
            table, attribs = match.groups()
            return table.strip()
        else:
            return ""
    elif query_type == MiniSQLType.DROP_TABLE:
        query = query.strip('; \n')
        match = re.match(r'^drop\s+table\s+(.+)$', query, re.S)
        if match:
            return match.group(1).strip()
        else:
            return ""


@app.route('/selectmaster', methods=['get'])
def selectmaster():
    query = request.args.get('query')
    querytype = judge_type(query)
    global table_region_list
    global server_list
    global region_load_list

    if querytype == MiniSQLType.DROP_INDEX:
        index_name = get_index_name(query)
        tmpff = 0
        for i in range(len(index_region_list)):
            if index_region_list[i]["indexname"] == index_name:
                tmpff = 1
                for j in range(len(server_list)):
                    if server_list[j]["region"] == index_region_list[i]["region"] and server_list[j]["isMaster"] == True:
                        del index_region_list[i]
                        return json.dumps(server_list[j])
        if tmpff == 0:
            return "1"
    table_name = get_table_name(query)
    print(query)
    if table_name == "":
        return "0"
    print("table name: ", table_name)
    if querytype == MiniSQLType.SELECT:
        for i in range(len(table_region_list)):
            if table_region_list[i]["tablename"] == table_name:
                # find = 0
                for j in range(len(region_load_list)):
                    if region_load_list[j]["region"] == table_region_list[i]["region"]:
                        region_load_list[j]["load"] += 1
                # if find == 0:
                #     region_load_list.append({"region": table_region_list[i]["region"], "load": 1})
                print("find table in list!")
                for j in range(len(server_list)):
                    if server_list[j]["region"] == table_region_list[i]["region"] and server_list[j]["isMaster"] == True:
                        return json.dumps(server_list[j])
    elif querytype == MiniSQLType.CREATE_TABLE:
        for i in range(len(table_region_list)):
            if table_name == table_region_list[i]["tablename"]:
                return "1"
        min = 100000
        min_index = -1
        for i in range(len(region_load_list)):
            if region_load_list[i]["load"]<min:
                min = region_load_list[i]["load"]
                min_index = i
        print("create table select region", min_index)
        if min_index == -1:
            min_index = 0
            for i in range(len(server_list)):
                tmpkk = 0
                for j in range(len(region_load_list)):
                    if region_load_list[j]["region"] == server_list[i]["region"]:
                        tmpkk = 1
                        break
                if tmpkk == 0:
                    region_load_list.append({"region": server_list[i]["region"], "load": 0})

        for i in range(len(server_list)):
            
            if server_list[i]["region"] == region_load_list[min_index]["region"] and server_list[i]["isMaster"] == True:
                table_region_list.append({"region": region_load_list[min_index]["region"], "tablename": table_name})
                return json.dumps(server_list[i])
            
    elif querytype == MiniSQLType.CREATE_INDEX:
        index_name = get_index_name(query)
        for i in range(len(index_region_list)):
            if index_name == index_region_list[i]["indexname"]:
                return "1"
        for i in range(len(table_region_list)):
            if table_region_list[i]["tablename"] == table_name:
                index_region_list.append({"region": table_region_list[i]["region"], "indexname": index_name})
                for j in range(len(server_list)):
                    if server_list[j]["region"] == table_region_list[i]["region"] and server_list[j]["isMaster"] == True:
                        return json.dumps(server_list[j])

    elif querytype == MiniSQLType.DROP_TABLE:
        tmpff = 0
        for i in range(len(table_region_list)):
            if table_region_list[i]["tablename"] == table_name:
                tmpff = 1
                for j in range(len(server_list)):
                    if server_list[j]["region"] == table_region_list[i]["region"] and server_list[j]["isMaster"] == True:
                        del table_region_list[i]
                        return json.dumps(server_list[j])
        if tmpff == 0:
            return "1"
    else:
        for i in range(len(table_region_list)):
            if table_region_list[i]["tablename"] == table_name:
                for j in range(len(server_list)):
                    if server_list[j]["region"] == table_region_list[i]["region"] and server_list[j]["isMaster"] == True:
                        return json.dumps(server_list[j])

    
    return "1"


@app.route('/selectregion', methods=['get'])
def selectregion():
    global server_list
    global rotation_lock
    orgin_lock = rotation_lock
    while 1:
        rotation_lock = rotation_lock + 1
        if rotation_lock >= len(server_list):
            rotation_lock = 0
        if server_lock_list[rotation_lock] == 0 and server_list[rotation_lock]["region"] == SELF_REGION:
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


def isleader():
    global server_list
    for i in server_list:
        if i["address"] == SELF_IP_PORT:
            if i["isLeader"] == True:
                return 1
            else:
                return 0


# 向所有节点广播写入信息
class WqueryThread(Thread):
    def __init__(self, id, query, address, lock, success, failed, update_lock):
        super(WqueryThread, self).__init__()
        self.id = id
        self.address = address
        self.query = query
        self.lock = lock
        self.success = success
        self.failed = failed
        self.update_lock = update_lock
    def run(self):
        global server_list
        global server_lock_list
        global server_real_lock_list
        response = 'f'
        server_lock_list[self.id] = 1
        server_real_lock_list[self.id].acquire()
        try:
            url = "http://" + self.address + "/query_broadcast"
            response = requests.get(url=url, params={"query": self.query}, timeout=1)
            # response = reponse.content
            # print(eval(response.content))
            response = eval(response.content)[0]
            # print(eval(response.content))
            # response = eval(response.content)[0]
            # print(response)
            # 返回值为数字即视为存活
            if response == '1':
                # print("okkk")
                self.update_lock.acquire()
                self.success.append(self.id)
                self.update_lock.release()
            else:
                self.update_lock.acquire()
                self.failed.append(self.id)
                self.update_lock.release()

        except Exception as e:
            # print(e)
            self.update_lock.acquire()
            self.failed.append(self.id)
            self.update_lock.release()
        
        if len(self.success) >= W_SUCCESS_THRESHOLD or len(self.failed) >= len(server_list) - W_SUCCESS_THRESHOLD:
            self.update_lock.acquire()
            self.lock.set()
            self.update_lock.release()
        
        self.lock.wait()
        # print("snum = ", len(self.success))
        if response == '1' and len(self.success) < W_SUCCESS_THRESHOLD:
            formdata = {"ip_port": server_list[self.failed[0]]["address"]}
            url = "http://" + self.address + "/synchronize_q"
            response = requests.get(url=url, params=formdata, timeout=1)
        if response != '1' and len(self.success) >= W_SUCCESS_THRESHOLD:
            formdata = {"ip_port": server_list[self.success[0]]["address"]}
            url = "http://" + self.address + "/synchronize_q"
            response = requests.get(url=url, params=formdata, timeout=1)
        
        server_lock_list[self.id] = 0
        server_real_lock_list[self.id].release()
        return

@app.route('/clear', methods=['get'])
def clear():
    global server_list
    global table_region_list
    global index_region_list

    query = request.args.get('query')
    formdata = {'query': query}
    
    table_region_list = []
    index_region_list = []
    for i in range(len(server_list)):
        if server_list[i]["isMaster"] == True:
            ret = requests.get("http://" + server_list[i]["address"] + "/wquery", params=formdata, timeout=1)
            if eval(ret.content) == 1:
                1
            else:
                # return "0"
                1
    return "1"


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
    # print(query)
    global server_list
    global server_lock_list
    write_lock.acquire()

    finish.clear()
    update_lock = Lock()
    for i in range(len(server_list)):
        if server_list[i]["region"] == SELF_REGION:
            pulse_thread = WqueryThread(i, query, server_list[i]["address"], finish, success_num, failed_num, update_lock)
            pulse_thread.start()
    
    finish.wait()
    write_lock.release()
    print("query success num when finished changes : ", len(success_num))
    if len(success_num) >= W_SUCCESS_THRESHOLD:
        return "1"
    else:
        return "0"


@app.route('/query_broadcast')
def query_broadcast():
    query = request.args.get('query')
    print(query)
    try:
        # beg = time.clock()
        ret = interpret(query, buf)
        # end = time.clock()
        # print(ret)
        return jsonify(ret)
        
    except MiniSQLError as e:
        return jsonify(e.args)

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
