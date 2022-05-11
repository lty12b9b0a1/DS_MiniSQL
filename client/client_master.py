from operator import le
import requests


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
query = ''
while True:
    print('MiniSQL->', end=' ')
    cmd = input()
    query += cmd + ' '
    cmd = cmd.strip()
    
    if cmd and cmd[-1] == ';':
        # print(query)
        # print("???")
        # print(query)
        formdata = {'query': query}
        query = ''
        try:
            ret = requests.get(url, params=formdata)


            tmp = eval(ret.content.strip())
            # print(tmp)
            if tmp == 0:
                break
            elif isinstance(tmp, (list, tuple)):
                # print(*(ret.content))
                if len(tmp) == 1:
                    print(tmp[0])
                else:

                    print(len(tmp))
                    print_table(tmp[0], tmp[1])
            else:
                print(tmp[0])

        except Exception as e:
            # print("ok")
            # print(e)
            1
