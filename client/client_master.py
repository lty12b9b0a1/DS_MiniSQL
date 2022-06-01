from ast import expr_context
from enum import Enum
import json
import requests




MiniSQLType = Enum('MiniSQLType', ('CREATE_TABLE', 'INSERT', 'DROP_TABLE', 'CREATE_INDEX',
                                   'DROP_INDEX', 'SELECT', 'DELETE', 'QUIT', 'EXECFILE', 'CLEAR', 'ERROR_TYPE'))


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

    return MiniSQLType.ERROR_TYPE


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


url = "http://127.0.0.1:23333/query"

curator_url = "127.0.0.1:5000"

query = ''
master_url = ""
leader_url = ""
while True:
    print('MiniSQL->', end=' ')
    cmd = input()
    if cmd == "exit":
        break

    query += cmd + ' '
    cmd = cmd.strip()

    if cmd and cmd[-1] == ';':
        # print(query)
        # print("???")
        # print(query)
        formdata = {'query': query}
        querytype = judge_type(query)
        # print(querytype)
        query = ''
        query_success = 0
        try:
            if querytype == MiniSQLType.QUIT:
                break

            # if leader_url == "":
            try:
                ret_leader_url = requests.get("http://" + curator_url + "/getLeader", timeout=1)
                leader_url = eval(ret_leader_url.text)
                # print(master_url)
            except Exception as e:
                print("wrong leader address from curator server")

            if querytype == MiniSQLType.SELECT:
                try:
                    ret_master = requests.get("http://" + leader_url + "/selectmaster", params=formdata, timeout=1)
                    try:
                        if eval(ret_master.content) == 0:
                            print("syntax error!")
                        elif eval(ret_master.content) == 1:
                            print("can not found suitable master server!")
                    except Exception as e:
                        ret_master_json = ret_master.json()
                        master_url = ret_master_json['address']
                        try:
                            ret_region = requests.get("http://" + master_url + "/selectregion", timeout=1)
                            ret_region_json = ret_region.json()
                            region_url = ret_region_json['address']
                            try:
                                ret = requests.get("http://" + region_url + "/query_broadcast", params=formdata)
                                query_success = 1
                                tmp = eval(ret.content.strip())
                                # print(tmp)
                                if tmp == 0:
                                    leader_url = ""
                                    break
                                elif isinstance(tmp, (list, tuple)):
                                    # print(*(ret.content))
                                    if len(tmp) == 1:
                                        leader_url = ""
                                        print(tmp[0])
                                    else:
                                        print_table(tmp[0], tmp[1])
                                else:
                                    leader_url = ""
                                    print(tmp[0])
                            except Exception as e:
                                print("query failed, please try again.")
                        except Exception as e:
                            print("get region address failed, please try again.")
                except Exception as e:
                    leader_url = ""
                    print(e)
                    print("get master address failed, please try again")
            elif querytype == MiniSQLType.CLEAR:
                try:
                    # print("hello?", master_url)
                    ret = requests.get("http://" + leader_url+ "/clear", params=formdata, timeout=1)
                    query_success = 1
                    # print("content:", eval(ret.content))
                    if eval(ret.content) == 1:
                        print("change success!")
                    else:
                        print("change fail!")
                except Exception as e:
                    leader_url = ""
                    print("query failed in master, please try again.")
            elif querytype != MiniSQLType.ERROR_TYPE:
                try:
                    ret_master = requests.get("http://" + leader_url + "/selectmaster", params=formdata, timeout=1)
                    try:
                        if eval(ret_master.content) == 0:
                            print("syntax error!")
                        elif eval(ret_master.content) == 1:
                            print("can not found suitable master server!")
                    except Exception as e:
                        ret_master_json = ret_master.json()
                        master_url = ret_master_json['address']
                        
                        try:
                            # print("hello?", master_url)
                            print(master_url)
                            ret = requests.get("http://" + master_url + "/wquery", params=formdata, timeout=1)
                            query_success = 1
                            print(ret.content)
                            # print("content:", eval(ret.content))
                            if eval(ret.content) == 1:
                                print("change success!")
                            else:
                                print("change fail!")
                        except Exception as e:
                            print("query failed in master, please try again.")
                except Exception as e:
                    leader_url = ""
                    print("query failed in leader, please try again.")
            else:
                raise TypeError("Syntax Error: Unrecognised Type")
            
            if query_success == 1:
                1
            else:
                master_url = ""
                print("something error during execute")

        except TypeError as e:
            master_url = ""
            print(e)
