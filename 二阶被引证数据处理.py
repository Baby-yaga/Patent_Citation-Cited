# -*- coding: utf-8 -*-
"""
Created on Sat Jul 26 22:01:46 2025

@author: ljh93
"""
from tqdm import tqdm
import pandas as pd
tqdm.pandas(desc='pandas bar')

# 数据路径
road = "E:/20250726专利数据-Rstata/"

# 读取和处理数据
all_data = []
citation_data_dict = {}
for year in tqdm(reversed(range(1985,2020))):
    # 用 chunksize 读取每年数据
    data_normal = pd.read_csv(road + "一阶被引证/{}.csv".format(str(year)), sep = "|")
    all_data = []
    for year_ in tqdm(range(year,2025)):
        # 读取引证数据
        first_citation_data = pd.read_csv(road + "专利引证数据_group_by_publicdate/publicdate_{}.csv".format(str(year_)), sep = "|")
        first_citation_data = first_citation_data[["公开公告号", '引证专利', '被引证专利']]
        
        batch_size = 100000  # 每批处理的行数
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
        
        # 二阶被引证
        try:
            data_normal_ = pd.merge(data_normal, citation_data_split, left_on = "被引证专利_1st", right_on = "公开公告号", how = "inner")
            data_normal_.columns = ['年份', '申请日', '申请号', '公开号', '公开日', '被引证专利_1st', '公开公告号', '被引证专利_2rd']
            data_normal_ = data_normal_[['年份', '申请日', '申请号', '公开号', '公开日', '被引证专利_1st', '被引证专利_2rd']]
            all_data.append(data_normal_)
        except:
            data_normal_ = pd.DataFrame(columns = ['年份', '申请日', '申请号', '公开号', '公开日', '被引证专利_1st', '被引证专利_2rd'])
            all_data.append(data_normal_)
      
    # 一次性保存每年合并后的结果
    all_data.append(data_normal)
    all_data_combined = pd.concat(all_data, ignore_index=True)
    all_data_combined = all_data_combined.sort_values(by=['被引证专利_1st','被引证专利_2rd'], ascending=True)
    all_data_combined = all_data_combined[~all_data_combined.duplicated()].reset_index(drop=True)
    
    # 找到重复的行（去掉空值的行），根据 'last_column' 来过滤
    all_data_combined = all_data_combined[~all_data_combined.duplicated(subset=all_data_combined.columns[:-1], keep='first') | all_data_combined['被引证专利_2rd'].notna()]
    
    # 如果需要，可以重新索引
    all_data_combined = all_data_combined.reset_index(drop=True)
    all_data_combined.to_csv(road + "二阶被引证/{}.csv".format(str(year)), index=False, sep = "|")
