# -*- coding: utf-8 -*-
"""
Created on Sat Jul 26 11:42:12 2025

@author: ljh93
"""

import os
import datetime
from tqdm import tqdm
import pandas as pd
tqdm.pandas(desc='pandas bar')

# 数据路径
input_road = "E:/20250726专利数据-Rstata/专利引证-拆分版/"
road = "E:/20250726专利数据-Rstata/专利引证数据_group_by_publicdate/"
# 读取和处理数据
for year in tqdm(range(2017,2022)):
    path = input_road + str(year) + "/"
    files = os.listdir(path)
    for file in tqdm(files):
        data_normal = pd.read_stata(path + file)
        data_normal = data_normal[['公开公告号', '公开公告日', '引证专利', '被引证专利']]
        data_normal['year'] = data_normal['公开公告日'].apply(lambda x:x[0:4])
        open_dates = list(set(data_normal['year'].to_list()))
        for date in open_dates:
            temp = data_normal[data_normal['year'] == date]
            
            # 获取当前系统时间
            current_time = datetime.datetime.now()
            
            # 格式化时间为一个字符串，确保文件名唯一
            unique_filename = current_time.strftime("%Y%m%d_%H%M%S_%f")
            
            temp.to_stata(road + "{}.dta".format(date+"_"+unique_filename), write_index = 0, version = 119)

# 设置文件夹路径
folder_path = 'E:/20250726专利数据-Rstata/专利引证数据_group_by_publicdate/'  # 替换成你自己的文件夹路径
# 获取文件夹中所有文件的文件名
all_files = os.listdir(folder_path)

for year in tqdm(range(2021,2025)):
    infos = pd.DataFrame()
    # 过滤出以 '1985' 开头的文件名
    filtered_files = [file for file in all_files if file.startswith(str(year))]
    for file in tqdm(filtered_files):
        data = pd.read_stata(folder_path + file)
        infos = pd.concat([infos,data])
    infos.to_stata(folder_path + "publicdate_{}.dta".format(str(year)), write_index = 0, version = 119)