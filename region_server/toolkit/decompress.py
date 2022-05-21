import zipfile
import os
"""
src_path:压缩包所在文件路径
target_path:压缩后文件存放路径
"""
def decompress(src_path, target_path):
    # src_path=r"\chrome\chromedriver_win32.zip"
    # target_path="\chrome\数据"
    if(not os.path.isdir(target_path)):
        z = zipfile.ZipFile(src_path, 'r')
        z.extractall(path=target_path)
        z.close()
