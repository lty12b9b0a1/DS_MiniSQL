import pickle


protocol = pickle.DEFAULT_PROTOCOL
table_schema_file = './DB/catalog/table_schema.minisql'
# table_file_prefix = 'DB/record/{}.minisql'
index_file_prefix = './DB/index/{}.index'
log_file = './DB/log.txt'

block_path = './DB/memory/{}.block'
block_dir = './DB/memory'
header_path = './DB/memory/header.hd'

BLOCK_SIZE = 4 * 256 * 1024
TOTAL_BLOCK = 1
TOTAL_BLOCK_IN_BUFFER = 1


TREE_ORDER = 10
MAX_RECORDS_PER_BLOCK = 100000


W_SUCCESS_THRESHOLD = 1
SELF_IP_PORT = "127.0.0.1:23333"
CURATOR_IP_PORT = "127.0.0.1:5000"
SELF_PORT = 23333

def log(msg):
    print(msg)
    with open(log_file, 'a') as f:
        f.write(msg + '\n')
