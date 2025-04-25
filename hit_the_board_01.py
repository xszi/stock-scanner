import akshare as ak
import pandas as pd


def get_non_st_stocks():
    """
    获取全部A股股票并过滤ST/*ST/SST股票
    返回：过滤后的DataFrame（包含代码、名称）
    """
    try:
        # 获取所有A股列表（包含代码、名称）
        stock_df = ak.stock_info_a_code_name()

        # 清洗数据：去除名称中的换行符
        stock_df["name"] = stock_df["name"].str.replace("\n", "", regex=False)

        # 过滤ST/*ST/SST股票（不区分大小写）
        pattern = r"ST[\*]*|SST"
        mask = ~stock_df["name"].str.contains(pattern, case=False, regex=True)

        filtered_df = stock_df[mask]

        return filtered_df

    except Exception as e:
        print(f"获取数据失败，错误信息：{e}")
        return None


# 调用函数并保存结果
if __name__ == "__main__":
    df = get_non_st_stocks()
    print(df[0], 'xxx')
    if df is not None:
        print(f"共获取到 {len(df)} 只非ST股票")
        df.to_csv("non_st_stocks.csv", index=False, encoding="utf-8-sig")
        print("结果已保存到 non_st_stocks.csv")
        # 打印前5条数据示例
        print(df.head())