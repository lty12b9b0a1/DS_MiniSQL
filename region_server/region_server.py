from enum import Enum
from http.client import responses
from pkg_resources import resource_listdir
import requests

from execption import MiniSQLSyntaxError
from API import *
from buffer_manager import *
import time
import json
from flask import Flask, request, jsonify, Response
from flask_cors import CORS

server_list = []
rotation_lock = 0
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
    with open("./testDB/memory/0.block", "wb") as f:
        f.write(ret.content)

    url = "http://"+ ip_port + "/testfile1"
    ret = requests.get(url)
    with open("./testDB/memory/header.hd", "wb") as f:
        f.write(ret.content)
        
    url = "http://"+ ip_port + "/testfile2"
    ret = requests.get(url)
    with open("./testDB/catalog/table_schema.minisql", "wb") as f:
        f.write(ret.content)

    url = "http://"+ ip_port + "/testfile3"
    ret = requests.get(url)
    with open("./testDB/log.txt", "wb") as f:
        f.write(ret.content)
    
    return 1


def register():
    formdata = {"ip_port": SELF_IP_PORT}
    try:
        server_ip_port_list = requests.get("http://"+CURATOR_IP_PORT+"/register", params=formdata)

        synchronize(server_ip_port_list[0])
    except Exception as e:
        print(e)


@app.route('/testfile0', methods=['get'])
def testfile():
    files = []
    result = []
    file = open('./DB/memory/0.block', 'rb')
    raw = file.read()
    # raw_str = bytes.decode(raw)
    # temp = {}
    # temp["file_content"] = raw_str
    # result.append(temp.copy())
    # files.append(raw)
    # # print(type(files))
    # # file = open('./DB/memory/header.hd', 'rb')
    # # raw = file.read()
    # # files.append(raw)
    # # file = open('./DB/catalog/table_schema.minisql', 'rb')
    # # files.append(file)
    # # file = open('./DB/log.txt', 'rb')
    # # files.append(file)
    return Response(raw)

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
    server_list = request.args.get('server_list')
    return json.dumps(1)

@app.route('/selectregion', methods=['get'])
def selectregion():
    global rotation_lock
    rotation_lock = rotation_lock + 1
    if rotation_lock >= len(server_list):
        rotation_lock = 0
    return jsonify(server_list[rotation_lock])


@app.route('/query', methods=['get'])
def query():
    query = request.args.get('query')
    print(query)
    try:
        beg = time.clock()
        ret = interpret(query, buf)
        end = time.clock()

        # if ret == 0:
        #     break
        # elif isinstance(ret, (list, tuple)):
        #     print_table(*ret)
        # print(ret)
        print(ret)
    except MiniSQLError as e:
        # ret = e
        # print(e.args)
        return jsonify(e.args)
        end = beg
    query = ''

    print('use time {}s'.format(end - beg))
    return jsonify(ret)

if __name__ == '__main__':
    # query = ''
    try:
        # register()
        buf = Buffer()
        app.run(host="0.0.0.0", port=SELF_PORT)
        buf.close()
    except Exception as e:
        print(e)
    # main()
