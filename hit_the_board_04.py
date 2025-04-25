import akshare as ak
import pandas as pd
import numpy as np


def get_limit_up_stocks():
    """获取涨停板股票（修复涨跌幅处理）"""
    try:
        # 使用更稳定的东财接口
        df = ak.stock_zh_a_spot_em()

        # 强制转换为字符串并处理特殊字符
        df['涨跌幅'] = df['涨跌幅'].astype(str).str.replace('%-$', '', regex=True)  # 处理可能的负百分比符号

        # 统一去除所有非数字字符（包括%）
        df['涨跌幅'] = df['涨跌幅'].str.replace('[^0-9.-]', '', regex=True)

        # 转换为数值类型（处理无效值为NaN）
        df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce')

        # 过滤无效数据
        df = df[df['最新价'] > 0].copy()

        # 市场类型分类
        conditions = [
            df['名称'].str.contains('ST'),
            df['代码'].str.startswith('688'),
            df['代码'].str[:3].isin(['300', '301']),
            df['代码'].str[:2].isin(['43']),
            df['代码'].str[:2].isin(['83', '87'])
        ]
        choices = ['ST股', '科创板', '创业板', '新三板', '北交所']
        df['涨停类型'] = np.select(conditions, choices, default='主板')

        # 定义涨停幅度
        limit_map = {
            'ST股': 0.05,
            '主板': 0.10,
            '创业板': 0.20,
            '科创板': 0.20,
            "北交所": 0.30,
            '新三板': 0.50,
        }
        df['涨停幅度'] = df['涨停类型'].map(limit_map)



        # 计算涨停阈值（处理浮点精度）
        df['涨停阈值'] = (df['今开'] * (1 + df['涨停幅度'])).round(2)

        # 筛选涨停股票
        mask = (df['最新价'] >= df['涨停阈值']) & (df['涨跌幅'] >= df['涨停幅度'] * 100)
        limit_up_df = df[mask].copy()

        # 计算封板指标
        # limit_up_df['封板量(手)'] = limit_up_df['买一量']  # 原始单位是手(100股/手)

        print(limit_up_df)
        return limit_up_df

        # 计算封单金额（处理千分位分隔符）
        # limit_up_df['买一量'] = limit_up_df['买一量'].str.replace(',', '').astype(float)
        # limit_up_df['封单金额(万)'] = (limit_up_df['买一量'] * limit_up_df['最新价']) / 10000
        # return limit_up_df.sort_values('封单金额(万)', ascending=False)

    except Exception as e:
        print(f"发生错误: {str(e)}")
        return pd.DataFrame()


# 使用示例
if __name__ == "__main__":
    result = get_limit_up_stocks()
    if not result.empty:
        print(result[['代码', '名称', '涨停类型', '涨跌幅']])
    else:
        print("未获取到有效数据")