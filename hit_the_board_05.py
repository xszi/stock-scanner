import akshare as ak
import pandas as pd
import numpy as np
import os
from datetime import datetime


def filter_by_cg_ratio(limit_up_stocks):
    """
    根据减持比例筛选
    """
    if os.path.exists("stock_cg.csv"):
        df = pd.read_csv("stock_cg.csv", low_memory=False)
        df = df[["代码", "持股变动信息-占总股本比例"]]  # 筛选列
        df.drop_duplicates(subset=["代码"], keep="first", inplace=True)  # 去重
        df.reset_index(drop=True, inplace=True)  # 重置索引
    else:
        limit_up_stocks["持股变动信息-占总股本比例"] = 0
        df = ak.stock_ggcg_em(symbol="股东减持")
        df.to_csv("stock_cg.csv")
    for index, row in limit_up_stocks.iterrows():
        for index1, row1 in df.iterrows():
            if int(limit_up_stocks.at[index, "代码"]) == df.at[index1, "代码"]:
                limit_up_stocks.at[index, "减持比例"] = df.at[
                    index1, "持股变动信息-占总股本比例"
                ]
    limit_up_stocks = limit_up_stocks[
        limit_up_stocks["减持比例"] <= 1
    ]  # 过滤掉减持比例大于1%
    return limit_up_stocks


def filter_by_tfp_info(limit_up_stocks):
    """
    根据警示信息筛选
    """
    if os.path.exists("stock_tfp_info.csv"):
        df = pd.read_csv("stock_tfp_info.csv", low_memory=False)
        df = df[["代码", "停牌原因"]]  # 筛选列
        df.drop_duplicates(subset=["代码"], keep="first", inplace=True)  # 去重
        df.reset_index(drop=True, inplace=True)  # 重置索引
    else:
        now = datetime.now().strftime("%Y%m%d")
        df = ak.stock_tfp_em(date=now)  # 警示板
        df.to_csv("stock_tfp_info.csv")
    for index, row in limit_up_stocks.iterrows():
        for index1, row1 in df.iterrows():
            if int(limit_up_stocks.at[index, "代码"]) == df.at[index1, "代码"]:
                limit_up_stocks.at[index, "是否警示"] = True
            else:
                limit_up_stocks.at[index, "是否警示"] = False
    limit_up_stocks = limit_up_stocks[limit_up_stocks["是否警示"] == False]
    print(limit_up_stocks)
    return limit_up_stocks


def filter_by_sy(limit_up_stocks):
    """
    根据商誉占净资产比率筛选
    """
    if os.path.exists("stock_sy.csv"):
        df = pd.read_csv("stock_sy.csv", low_memory=False)
        df = df[["股票代码", "商誉占净资产比例"]]  # 筛选列
        df.drop_duplicates(subset=["股票代码"], keep="first", inplace=True)  # 去重
        df.reset_index(drop=True, inplace=True)  # 重置索引
    else:
        now = datetime.now().strftime("%Y%m%d")
        df = ak.stock_sy_em(date="20240630")  # 商誉+净利润
        df.to_csv("stock_sy.csv")

    for index, row in limit_up_stocks.iterrows():
        for index1, row1 in df.iterrows():
            if int(limit_up_stocks.at[index, "代码"]) == df.at[index1, "股票代码"]:
                limit_up_stocks.at[index, "商誉占净资产比例"] = df.at[
                    index1, "商誉占净资产比例"
                ]
    limit_up_stocks = limit_up_stocks[limit_up_stocks["商誉占净资产比例"] < 0.2]
    return limit_up_stocks


def filter_by_net_income_current_ratio(limit_up_stocks):
    # 获取涨停股票的净利润和流动比率
    for index, row in limit_up_stocks.iterrows():
        result = ak.stock_financial_abstract_ths(
            symbol=row["代码"], indicator="按报告期"
        )
        limit_up_stocks.at[index, "净利润"] = result.tail(1)["净利润"].iloc[0]
        limit_up_stocks.at[index, "流动比率"] = float(
            result.tail(1)["流动比率"].iloc[0]
        )
        # limit_up_stocks.at[index, "应收账款周转天数"] = float(
        #     result.tail(1)["应收账款周转天数"].iloc[0]
        # )
        # limit_up_stocks.at[index, "应收账款周转率"] = 365 / float(
        #     result.tail(1)["应收账款周转天数"].iloc[0]
        # )
        limit_up_stocks.at[index, "应收/营收"] = (
            float(result.tail(1)["应收账款周转天数"].iloc[0]) / 365
        )
    limit_up_stocks = limit_up_stocks[~limit_up_stocks["净利润"].str.startswith("-")]
    limit_up_stocks = limit_up_stocks[limit_up_stocks["流动比率"] > 1]
    limit_up_stocks = limit_up_stocks[limit_up_stocks["应收/营收"] < 0.3]
    limit_up_stocks.to_csv("filter_limit_up_stocks.csv")
    print(limit_up_stocks)
    return limit_up_stocks


def add_quantity_info_for_stocks(df):
    """
    获取早盘量比和封板量, 封成比
    """
    stock_zh_a_spot_em_df = (
        ak.stock_zh_a_spot_em()
    )  # 实时行情数据里面包含量比，9:00 - 9:25 获取到该时间段的早盘量比
    # 量比值             市场含义        操作信号
    # 量比 < 0.5        成交极度清淡，    市场关注度低观望，警惕流动性风险
    # 0.5 ≤ 量比 < 1    成交正常，       无显著资金异动维持原有策略
    # 1 ≤ 量比 < 3      成交温和放大，    可能启动趋势关注突破机会，结合价格走势判断
    # 3 ≤ 量比 < 5      成交显著活跃，    主力资金介入高概率出现单边行情，可择机跟进
    # 量比 ≥5           成交异常放大，    短期情绪极端化警惕主力出货或消息刺激，快进快出
    # 过滤掉ST涨停股票
    mask = ~stock_zh_a_spot_em_df["名称"].str.contains("ST")
    filter_stocks = stock_zh_a_spot_em_df[mask].copy()

    for index, row in stock_zh_a_spot_em_df.iterrows():
        for index1, row1 in df.iterrows():
            if row["代码"] == row1["代码"]:
                df.at[index1, "成交量"] = row["成交量"]  # 单位手
                df.at[index1, "量比"] = row["量比"]

    df["封板量"] = df["封板资金"] / df["最新价"] / 100
    # 封成比的意义
    # 反映封板强度
    # 封成比 > 1：封板资金远超当日成交量，说明主力资金锁仓意愿强，次日大概率延续涨停或跌停。
    # 封成比 < 0.5：封板资金不足，封板不牢固，容易被抛压打开。
    # 辅助判断短期走势
    # 高封成比（如封成比 > 2）：股价次日高开概率大，甚至可能连续涨停。
    # 低封成比（如封成比 < 0.3）：次日可能低开或开板回调。
    df["封成比"] = df["封板量"] / df["成交量"]
    print(df, "888")


def add_score_column(df):
    """
    股票评分
    """
    df["评分"] = 0
    for index, row in df.iterrows():
        if (row["最后封板时间"] <= 103000) & (row["封单量"] >= 12000000):
            df.at[index, "评分"] = df.at[index, "评分"] + 20
    print(df, "mmm")


def get_limit_up_stocks():
    """获取涨停板股票"""
    try:
        now = datetime.now().strftime("%Y%m%d")
        limit_up_stocks = ak.stock_zt_pool_em(date=now)
        # 转换为数值类型（处理无效值为NaN）
        limit_up_stocks["最后封板时间"] = pd.to_numeric(
            limit_up_stocks["最后封板时间"], errors="coerce"
        )
        # 过滤掉ST涨停股票
        mask = ~limit_up_stocks["名称"].str.contains("ST")
        limit_up_stocks = limit_up_stocks[mask].copy()
        # limit_up_stocks = filter_by_cg_ratio(
        #     limit_up_stocks
        # )  # 通过减持比例过滤 过滤掉近六月大股东减持 > 1 %
        # limit_up_stocks = filter_by_tfp_info(limit_up_stocks)  # 通过警示版信息过滤
        # limit_up_stocks = filter_by_sy(limit_up_stocks)  # 根据商誉占净资产比率过滤
        # limit_up_stocks = filter_by_net_income_current_ratio(
        #     limit_up_stocks
        # )  # 流动比率+净利润

        # 开始通过5个维度给涨停股打分
        # print(limit_up_stocks, "ggg")
        # 量价
        limit_up_stocks = add_quantity_info_for_stocks(limit_up_stocks)
        return limit_up_stocks

    except Exception as e:
        print(f"发生错误: {str(e)}")
        return pd.DataFrame()


# 使用示例
if __name__ == "__main__":
    result = get_limit_up_stocks()
    # if not result.empty:
    #     print("999")
    # else:
    #     print("未获取到有效数据")
