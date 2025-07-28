# -*- coding: utf-8 -*-
"""
Created on Sat Jul 26 09:48:28 2025

@author: ljh93
"""
from tqdm import tqdm
import pandas as pd
tqdm.pandas(desc='pandas bar')

# 数据路径
road = "E:/20250726专利数据-Rstata/"
# 读取和处理数据
for year in tqdm(range(1985,2025)):
    data_normal = pd.read_stata(road + "专利数据_group_by_publicdate/publicdate_{}.dta".format(str(year)))
    data_normal = data_normal[['year', '申请日', '申请号', '公开号', '公开日']]
    
    # 读取引证数据
    first_citation_data = pd.read_csv(road + "专利引证数据_group_by_publicdate/publicdate_{}.csv".format(str(year)), sep = "|")
    first_citation_data = first_citation_data[["公开公告号", '引证专利', '被引证专利']]
    batch_size = 100000  # 每批处理的行数
    # 处理被引证专利列
    # 分批次处理
    chunks = []
    for start in range(0, len(first_citation_data), batch_size):
        chunk = first_citation_data.iloc[start:start + batch_size]
        
        # 处理当前批次
        chunk_split = chunk.drop('被引证专利', axis=1).join(
            chunk['被引证专利'].str.split('; ', expand=True).stack().reset_index(level=1, drop=True).rename('被引证专利_split')
        )
        
        chunks.append(chunk_split)
    # 合并所有批次的结果
    citation_data_split = pd.concat(chunks, ignore_index=True)
    
    citation_data_split.index = range(len(citation_data_split))
    citation_data_split = citation_data_split[["公开公告号", '被引证专利_split']]

    # 合并一阶引证
    data_normal_ = pd.merge(data_normal, citation_data_split, left_on = "公开号", right_on = "公开公告号", how = "left")
    data_normal_ = data_normal_[['year', '申请日', '申请号', '公开号', '公开日', '被引证专利_split']]
    data_normal_.columns = ['年份', '申请日', '申请号', '公开号', '公开日', '被引证专利_1st']
        
    data_normal_.to_csv(road + "一阶被引证/{}.csv".format(str(year)), index=False, sep = "|")
