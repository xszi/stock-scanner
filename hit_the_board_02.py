import akshare as ak
import pandas as pd
import time
from datetime import datetime


def get_non_st_positive_net_profit_stocks():
    """
    获取近一年净利润 > 0 的非 ST 股票
    返回：包含代码、名称、净利润的 DataFrame
    """
    try:
        # 1. 获取所有 A 股列表，过滤 ST/*ST/SST 股票
        stock_df = ak.stock_info_a_code_name()
        stock_df["name"] = stock_df["name"].str.replace("\n", "", regex=False)
        pattern = r"ST[\*]*|SST"
        mask = ~stock_df["name"].str.contains(pattern, case=False, regex=True)
        filtered_stocks = stock_df[mask]

        codes = filtered_stocks["code"].tolist()[1:5]

        # 2. 定义报告期（动态获取上年度年报日期，例如 20221231）
        # current_year = datetime.now().year
        # report_date = f"{current_year - 1}1231"  # 上年度年报截止日期
        # 3. 遍历股票，获取财务数据并过滤
        valid_stocks = []
        for code in codes:
            try:
                # 获取财务摘要数据（年度）
                df = ak.stock_financial_abstract(
                    symbol=code
                )
                if not df.empty:
                    net_profit = df.iloc[0, 2]  # 归母净利润
                    if net_profit > 0:
                        # 获取股票名称
                        name = filtered_stocks.loc[filtered_stocks["code"] == code, "name"].values[0]
                        valid_stocks.append({
                            "code": code,
                            "name": name,
                            "net_profit": net_profit
                        })
                # 控制请求频率，避免被封
                time.sleep(0.5)

            except Exception as e:
                print(f"获取 {code} 数据失败: {e}")
                continue

        # 4. 转换为 DataFrame 并返回
        result_df = pd.DataFrame(valid_stocks)
        print(result_df, 'jjj')
        return result_df

    except Exception as e:
        print(f"主程序异常: {e}")
        return pd.DataFrame()


# 调用函数并保存结果
if __name__ == "__main__":
    df = get_non_st_positive_net_profit_stocks()
    if not df.empty:
        print(f"共获取到 {len(df)} 只净利润 > 0 的非 ST 股票")
        df.to_csv("non_st_positive_net_profit.csv", index=False, encoding="utf-8-sig")
        print("结果已保存到 non_st_positive_net_profit.csv")
        # 打印前 5 条数据示例
        print(df.head())
    else:
        print("未获取到符合条件的数据")