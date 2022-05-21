import zipfile
import os

# with zipfile.ZipFile('log.zip', 'w') as z:
#     z.write('log.log')

def compress_file(zipfilename, dirname):      # zipfilename是压缩包名字，dirname是要打包的目录
    if os.path.isfile(dirname):
        with zipfile.ZipFile(zipfilename, 'w') as z:
            z.write(dirname)
    else:
        with zipfile.ZipFile(zipfilename, 'w') as z:
            for root, dirs, files in os.walk(dirname):
                for single_file in files:
                    if single_file != zipfilename:
                        filepath = os.path.join(root, single_file)
                        z.write(filepath)

# compress_file('tmp.zip', '.')      # 执行函数