# 北京时间转时间戳
import time

str_time = "2010/06/29 00:00:00"
time_tuple_2 = time.strptime(str_time, "%Y/%m/%d %H:%M:%S")
time_stample = time.mktime(time_tuple_2)
print("时间戳：", int(time_stample))

