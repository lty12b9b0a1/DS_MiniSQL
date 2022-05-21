from doctest import master
from enum import Enum
from operator import le
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

curator_url = "http://127.0.0.1:5000"

query = ''
master_url = ""
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

            if master_url == "":
                ret_master_url = requests.get(curator_url + "/getMaster", timeout=1)
                try:
                    master_url = eval(ret_master_url.text)
                    # print(master_url)
                except Exception as e:
                    print("wrong master address from curator server")
            if querytype == MiniSQLType.SELECT:
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
                            master_url = ""
                            break
                        elif isinstance(tmp, (list, tuple)):
                            # print(*(ret.content))
                            if len(tmp) == 1:
                                master_url = ""
                                print(tmp[0])
                            else:
                                print_table(tmp[0], tmp[1])
                        else:
                            master_url = ""
                            print(tmp[0])
                    except Exception as e:
                        print("query failed, please try again.")
                except Exception as e:
                    print("get region address failed, please try again.")

            elif querytype != MiniSQLType.ERROR_TYPE:
                try:
                    ret = requests.get("http://" + master_url + "/wquery", params=formdata)
                    query_success = 1
                    # print("content:", eval(ret.content))
                    if eval(ret.content) == 1:
                        print("change success!")
                    else:
                        print("change fail!")
                except Exception as e:
                    print("query failed, please try again.")
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
