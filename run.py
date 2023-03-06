# pip3 install mysql-connector-python==8.0.31
import os
import re
import sys

import mysql.connector
import time
import configs
import hashlib
import difflib

start_time = time.time()

# 清除下历史文件
if os.path.exists("compare.html"):
    os.remove("compare.html")

# 源端
source_db = mysql.connector.connect(
    host=configs.mysql_source_host,
    port=configs.mysql_source_port,
    user=configs.mysql_source_user,
    passwd=configs.mysql_source_pass,
)
source_cursor = source_db.cursor()

# 目标端
dest_db = mysql.connector.connect(
    host=configs.mysql_dest_host,
    port=configs.mysql_dest_port,
    user=configs.mysql_dest_user,
    passwd=configs.mysql_dest_pass,
)
dest_cursor = dest_db.cursor()


# 1 获取源端的表清单
get_src_tb_list = (
        "SELECT table_name FROM information_schema.tables WHERE table_schema="
        + "'" + configs.mysql_source_db + "'"
)
source_cursor.execute(get_src_tb_list)
src_tb_result = source_cursor.fetchall()

src_tb_set = set()
for x in src_tb_result:
    tb_name = x[0]
    src_tb_set.add(tb_name)
print(f"源端的表明细：", src_tb_set)

# 2 获取目标端的表清单
get_dest_tb_list = (
        "SELECT table_name FROM information_schema.tables WHERE table_schema="
        + "'" + configs.mysql_dest_db + "'"
)
dest_cursor.execute(get_dest_tb_list)
dest_tb_result = dest_cursor.fetchall()

dest_tb_set = set()
for x in dest_tb_result:
    tb_name = x[0]
    dest_tb_set.add(tb_name)
print(f"目标端的表明细：", dest_tb_set)

print("----------- 计算源端和目标端的差集 --------------------")
print(f"dest比src多的表：", list(dest_tb_set - src_tb_set))
print(f"src比dest多的表：", list(src_tb_set - dest_tb_set))


# print("------------ 开始比较每个表的每个列信息 -------------------")

if configs.check_charset_flag:
    base_sql = '''
    SELECT
     table_name,
      column_name,
     CASE WHEN `COLUMN_DEFAULT` IS NULL THEN '' ELSE `COLUMN_DEFAULT` END AS `COLUMN_DEFAULT` ,
      IS_NULLABLE,
      COLUMN_TYPE,
      CASE WHEN CHARACTER_SET_NAME IS NULL THEN '' ELSE CHARACTER_SET_NAME END AS CHARACTER_SET_NAME ,
      CASE WHEN COLLATION_NAME IS NULL THEN '' ELSE COLLATION_NAME END AS COLLATION_NAME ,
      COLUMN_KEY,
      EXTRA
    FROM
      information_schema.columns where table_schema =
    '''
else:
    base_sql = '''
        SELECT
         table_name,
          column_name,
         CASE WHEN `COLUMN_DEFAULT` IS NULL THEN '' ELSE `COLUMN_DEFAULT` END AS `COLUMN_DEFAULT` ,
          IS_NULLABLE,
          COLUMN_TYPE,
          COLUMN_KEY,
          EXTRA
        FROM
          information_schema.columns where table_schema = 
        '''

source_chksum = dict()
dest_chksum = dict()

# 3 采集源库数据
for i in src_tb_set:
    get_src_tb_column_detail = (
            base_sql
            + "'" + configs.mysql_source_db + "'" + "AND table_name=" + "'" + str(
        i) + "' ORDER BY ordinal_position ASC;"
    )

    source_cursor.execute(get_src_tb_column_detail)
    src_tb_column_result = source_cursor.fetchall()

    chk_sum = hashlib.md5(str(src_tb_column_result).encode()).hexdigest()
    source_chksum[i] = chk_sum

# 4 采集目标库信息
for i in dest_tb_set:
    get_dest_tb_column_detail = (
            base_sql
            + "'" + configs.mysql_dest_db + "'" + "AND table_name=" + "'" + str(i) + "' ORDER BY ordinal_position ASC;"
    )

    dest_cursor.execute(get_dest_tb_column_detail)
    dest_tb_column_result = dest_cursor.fetchall()

    chk_sum = hashlib.md5(str(dest_tb_column_result).encode()).hexdigest()
    dest_chksum[i] = chk_sum


# 5 进行集合运算
print("-------------- 结果统计 ------------------")
s1 = set()
if source_chksum != dest_chksum:

    differ = set(source_chksum.items()) ^ set(dest_chksum.items())

    for i in differ:
        # 如果要排除掉src和dest 存在差集的表（背景：有时候源库src已经建好表，但是尚未发布到生产dest去，这种情况下就出现了二者表的数量不一样多），用下面这种写法
        # if i[0] not in list(dest_tb_set  - src_tb_set ) and i[0] not in list(src_tb_set - dest_tb_set):
        #     s1.add(i[0])

        # 如果要全部都报出来，用下面这种写法
        s1.add(i[0])

    print('表结构检查完成，存在差异的表如下 ---> ', s1)
else:
    print('表结构检查完成，没有发现存在差异的表')



if len(s1) == 0:
    print("未发现差异的表")
else:
    # 6 对上面采集到的有差异的表，分别在src和dest上执行 show create table 操作
    # 先创建个空文件，防止在结果集为空的情况下，导致后面的diff比较报错
    with open("src_tb_struct.txt", 'a') as f:
        os.utime("src_tb_struct.txt", None)
    with open("dest_tb_struct.txt", 'a') as f:
        os.utime("dest_tb_struct.txt", None)

    for tb in s1:
        show_create_tb_sql = "SHOW CREATE TABLE " + str(tb)
        try:
            source_cursor.execute("use " + configs.mysql_source_db)
            source_cursor.execute(show_create_tb_sql)
            show_create_tb_result = source_cursor.fetchall()
            # print(show_create_tb_result)
            for ii in show_create_tb_result:
                with open("src_tb_struct.txt", "a+") as f:
                    # f.write(str(ii[1], ) + "\n")
                    f.write(str(re.sub('AUTO_INCREMENT=\d+', '', str(ii[1])), ) + "\n")
        except Exception as e:
            print(str(e))
            continue

    for tb in s1:
        show_create_tb_sql = "SHOW CREATE TABLE " + str(tb)
        try:
            dest_cursor.execute("use " + configs.mysql_dest_db)
            dest_cursor.execute(show_create_tb_sql)
            show_create_tb_result = dest_cursor.fetchall()
            # print(show_create_tb_result)
            for ii in show_create_tb_result:
                with open("dest_tb_struct.txt", "a+") as f:
                    # f.write(str( ii[1], ) + "\n")
                    f.write(str(re.sub('AUTO_INCREMENT=\d+', '', str(ii[1])), ) + "\n")
        except Exception as e:
            print(str(e))
            continue


    # 7 调用 difflib ， 生成比对文件
    src_result = open("src_tb_struct.txt", "r").readlines()
    dest_result = open("dest_tb_struct.txt", "r").readlines()
    difference = difflib.HtmlDiff(tabsize=2)
    with open("compare.html", "w") as fp:
        if sys.platform == 'linux':
            html = difference.make_file(fromlines=src_result, tolines=dest_result, fromdesc="source", todesc="destination")
        else:
            html = difference.make_file(fromlines=src_result, tolines=dest_result, fromdesc="源端", todesc="目标端",charset='gbk')
        fp.write(html)

    # 8 删除临时文件
    try:
        os.remove("src_tb_struct.txt")
    except Exception as e:
        print(str(e))
    try:
        os.remove("dest_tb_struct.txt")
    except Exception as e:
        print(str(e))

# 9 统计下耗时
stop_time = time.time()
time_dur = stop_time - start_time
print(f"耗时 {time_dur} 秒")

