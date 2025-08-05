# -*- coding: utf-8 -*-
"""
Created on Mon Aug  4 17:57:29 2025

@author: ljh93
"""

import pandas as pd
from tqdm import tqdm
road = "E:/20250726专利数据-Rstata/"

# road = "E:/20250726专利数据-Rstata/Result_二阶被引证/"
for year in tqdm(range(1985,2025)):
    data = pd.read_stata(road + "Result_二阶引证/result_{}.dta".format(str(year - 1985)))
    df = pd.read_stata(road + "专利数据_group_by_publicdate/publicdate_{}.dta".format(str(year)))
    df_merged = pd.merge(data, df, on = ["申请号",'申请日','公开号','公开日'], how = "left")
    df_merged = df_merged.drop('国民经济分类', axis=1).join(
        df_merged['国民经济分类'].str.split(';', expand=True).stack().reset_index(level=1, drop=True).rename('国民经济分类_split')
    )
    df_merged["国民经济分类_3位码"] = df_merged['国民经济分类_split'].apply(lambda x:x[0:4])
    
    # 删除某一列 (假设删除列 'B')
    df_merged = df_merged.drop('国民经济分类_split', axis=1)
    
    # 删除重复的行
    df_merged = df_merged.drop_duplicates()
    df_merged.to_stata(road + "城市-行业3位-二阶引证/publicdate_{}.dta".format(str(year)), write_index = 0, version = 119)

for year in tqdm(range(1985,2025)):
    data = pd.read_stata(road + "Result_二阶被引证/result_{}.dta".format(str(year - 1985)))
    df = pd.read_stata(road + "专利数据_group_by_publicdate/publicdate_{}.dta".format(str(year)))
    df_merged = pd.merge(data, df, on = ["申请号",'申请日','公开号','公开日'], how = "left")
    df_merged = df_merged.drop('国民经济分类', axis=1).join(
        df_merged['国民经济分类'].str.split(';', expand=True).stack().reset_index(level=1, drop=True).rename('国民经济分类_split')
    )
    df_merged["国民经济分类_3位码"] = df_merged['国民经济分类_split'].apply(lambda x:x[0:4])
    
    # 删除某一列 (假设删除列 'B')
    df_merged = df_merged.drop('国民经济分类_split', axis=1)
    
    # 删除重复的行
    df_merged = df_merged.drop_duplicates()
    df_merged.to_stata(road + "城市-行业3位-二阶被引证/publicdate_{}.dta".format(str(year)), write_index = 0, version = 119)
