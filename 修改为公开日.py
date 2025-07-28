# -*- coding: utf-8 -*-
"""
Created on Sat Jul 26 10:13:50 2025

@author: ljh93
"""
import os
import datetime
from tqdm import tqdm
import pandas as pd
tqdm.pandas(desc='pandas bar')

# 数据路径
input_road = "E:/20240608+专利数据+合并版本/"
road = "E:/20250726专利数据-Rstata/"
# 读取和处理数据
for year in tqdm(range(1985,2022)):
    data_normal = pd.read_csv(input_road + "{}.csv".format(str(year)),sep = "|")
    data_normal = data_normal[['申请日', '申请号', '公开号', '公开日']]
    data_normal['year'] = data_normal['公开日'].apply(lambda x:x[0:4])
    open_dates = list(set(data_normal['year'].to_list()))
    for date in open_dates:
        temp = data_normal[data_normal['year'] == date]
        
        # 获取当前系统时间
        current_time = datetime.datetime.now()
        
        # 格式化时间为一个字符串，确保文件名唯一
        unique_filename = current_time.strftime("%Y%m%d_%H%M%S_%f")
        
        temp.to_stata(road + "专利数据_group_by_publicdate/{}.dta".format(date+"_"+unique_filename), write_index = 0, version = 119)


# 设置文件夹路径
folder_path = 'E:/20250726专利数据-Rstata/专利数据_group_by_publicdate/'  # 替换成你自己的文件夹路径
# 获取文件夹中所有文件的文件名
all_files = os.listdir(folder_path)

for year in tqdm(range(1985,2025)):
    infos = pd.DataFrame()
    # 过滤出以 '1985' 开头的文件名
    filtered_files = [file for file in all_files if file.startswith(str(year))]
    for file in tqdm(filtered_files):
        data = pd.read_stata(folder_path + file)
        infos = pd.concat([infos,data])
    infos.to_stata(folder_path + "publicdate_{}.dta".format(str(year)), write_index = 0, version = 119)