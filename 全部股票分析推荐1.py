""""
Stock Analysis System
优化后的全盘股票技术分析系统——用于A股市场股票的全面分析，已加速分析并增加额外指标。
"""

import os
import time
import random
import logging
import traceback
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass

import numpy as np
import pandas as pd
import akshare as ak
from tqdm import tqdm

# -------------------------------
# **技术指标配置**
# -------------------------------
@dataclass # 用于自动为用户自定义的类添加生成的 特殊方法 例如 __init__() 和 __repr__()
class TechnicalParams:
    """技术指标参数配置"""
    ma_periods: Dict[str, int]
    rsi_period: int
    bollinger_period: int
    bollinger_std: int
    volume_ma_period: int
    atr_period: int

    @classmethod
    def default(cls) -> 'TechnicalParams':
        """返回默认的技术指标参数"""
        return cls(
            ma_periods={'short': 5, 'medium': 20, 'long': 60},
            rsi_period=14,
            bollinger_period=20,
            bollinger_std=2,
            volume_ma_period=20,
            atr_period=14
        )

# -------------------------------
# **股票分析引擎**
# -------------------------------
class StockAnalyzer:
    """股票分析引擎，计算各类技术指标"""

    def __init__(self, params: Optional[TechnicalParams] = None):
        """
        初始化股票分析引擎

        Args:
            params: 技术指标配置参数
        """
        self._setup_logging()
        self.params = params or TechnicalParams.default()

    def _setup_logging(self) -> None:
        """配置日志记录"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)

    def get_stock_data(self, stock_code: str,
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> pd.DataFrame:
        """
        获取单只股票历史数据，默认使用前一年的数据。

        Args:
            stock_code: 股票代码（可以带市场前缀或纯代码）
            start_date: 开始日期(格式YYYYMMDD)
            end_date: 结束日期(格式YYYYMMDD)

        Returns:
            包含日期、开盘、收盘、最高、最低、成交量的 DataFrame
        """
        try:
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')

            code = stock_code[2:] if stock_code.startswith(('sz', 'sh')) else stock_code

            df = ak.stock_zh_a_hist(
                symbol=code,
                start_date=start_date,
                period="daily",
                end_date=end_date,
                adjust="qfq"
            )

            self.logger.info(f"获取到 {len(df)} 行数据，列名：{df.columns.tolist()}")

            print(df)

            df = df.rename(columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "trade_date": "date"
            })
            print(set(df.columns), '8888')
            required_columns = {'date', 'open', 'close', 'high', 'low', 'volume'}
            missing_columns = required_columns - set(df.columns) # set去重
            if missing_columns:
                raise ValueError(f"缺失必须字段: {missing_columns}")

            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            numeric_columns = ['open', 'close', 'high', 'low', 'volume']
            df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
            # 去掉date为NaN的行数据
            df_cleaned = df.dropna(subset=['date'] + numeric_columns).sort_values('date')

            if len(df_cleaned) < 60:
                raise ValueError(f"数据不足（仅 {len(df_cleaned)} 行），无法计算至少60日均线")

            return df_cleaned

        except Exception as e:
            self.logger.error(f"获取股票数据失败，股票代码 {stock_code}，错误信息：{str(e)}")
            raise ValueError(f"股票 {stock_code} 数据获取出错: {str(e)}")
    @staticmethod
    def calculate_ema(series: pd.Series, period: int) -> pd.Series:
        """计算 EMA(指数移动平均线)"""
        return series.ewm(span=period, adjust=False).mean()

    @staticmethod
    def calculate_rsi(series: pd.Series, period: int) -> pd.Series:
        """
        计算 RSI（指数加权移动平均法）
        """
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
        avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
        rs = avg_gain / (avg_loss + 1e-10)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def calculate_macd(series: pd.Series) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """计算 MACD、信号线和直方图"""
        exp1 = series.ewm(span=12, adjust=False).mean()
        exp2 = series.ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9, adjust=False).mean()
        return macd, signal, macd - signal

    @staticmethod
    def calculate_bollinger_bands(series: pd.Series, period: int, std_dev: int) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """计算 Bollinger 通道"""
        middle = series.rolling(window=period, min_periods=period).mean()
        std = series.rolling(window=period, min_periods=period).std()
        upper = middle + std * std_dev
        lower = middle - std * std_dev
        return upper, middle, lower

    def calculate_atr(self, df: pd.DataFrame, period: int) -> pd.Series:
        """计算 ATR"""
        high = df['high']
        low = df['low']
        prev_close = df['close'].shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ], axis=1).max(axis=1)
        return tr.rolling(window=period, min_periods=period).mean()

    @staticmethod
    def calculate_obv(series_close: pd.Series, series_volume: pd.Series) -> pd.Series:
        """计算 OBV（能量潮指标）"""
        diff = series_close.diff().fillna(0)
        obv = np.where(diff > 0, series_volume, np.where(diff < 0, -series_volume, 0))
        return pd.Series(obv, index=series_close.index).cumsum()

    @staticmethod
    def calculate_stochastic(series_close: pd.Series, window: int = 14) -> Tuple[pd.Series, pd.Series]:
        """
        计算随机指标（Stochastic Oscillator）
           %K = (close - lowest_low) / (highest_high - lowest_low)*100
           %D 为 %K 的3日简单移动平均
        """
        lowest = series_close.rolling(window=window, min_periods=window).min()
        highest = series_close.rolling(window=window, min_periods=window).max()
        percentK = (series_close - lowest) / (highest - lowest + 1e-10) * 100
        percentD = percentK.rolling(window=3, min_periods=3).mean()
        return percentK, percentD

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算所有技术指标，并增加 OBV 和随机指标"""
        try:
            # 移动均线
            # MA5: 过去5个交易日收盘价的平均值连线，反映短期价格趋势。
            # MA5, MA20, MA60
            for key, period in self.params.ma_periods.items():
                df[f'MA{period}'] = self.calculate_ema(df['close'], period)
            df['RSI'] = self.calculate_rsi(df['close'], self.params.rsi_period)
            df['MACD'], df['Signal'], df['MACD_hist'] = self.calculate_macd(df['close'])
            df['BB_upper'], df['BB_middle'], df['BB_lower'] = self.calculate_bollinger_bands(
                df['close'], self.params.bollinger_period, self.params.bollinger_std)
            df['Volume_MA'] = df['volume'].rolling(window=self.params.volume_ma_period,
                                                   min_periods=self.params.volume_ma_period).mean()
            df['Volume_Ratio'] = df['volume'] / (df['Volume_MA'] + 1e-10)
            df['ATR'] = self.calculate_atr(df, self.params.atr_period)
            df['Volatility'] = df['ATR'] / df['close'] * 100
            df['ROC'] = df['close'].pct_change(periods=10) * 100

            # 增加 OBV 指标
            df['OBV'] = self.calculate_obv(df['close'], df['volume'])
            df['OBV_MA10'] = df['OBV'].rolling(window=10, min_periods=10).mean()

            # 增加随机指标 Stochastic
            df['%K'], df['%D'] = self.calculate_stochastic(df['close'], window=14)
            df.to_csv('D:/xszi\stock-scanner/scanner/finance2021.csv')
            return df

        except Exception as e:
            self.logger.error(f"指标计算出错：{str(e)}")
            raise

    def calculate_score(self, df: pd.DataFrame) -> float:
        """
        计算股票综合打分（基本打分满分100分），并根据 OBV 与随机指标调整±5分，
        使量化结果更全面。
        基本打分逻辑不变：
          趋势（30分）、RSI（20分）、MACD（20分）、成交量（30分）
        附加指标调整：
          OBV：OBV > OBV_MA10，+5分；反之，-5分；
          随机指标：%K < 20为超卖，+5分；%K > 80为超买，-5分。
        """
        try:
            score = 0
            latest = df.iloc[-1]
            # 趋势打分
            if latest['MA5'] > latest['MA20'] and latest['MA20'] > latest['MA60']:
                score += 30
            else:
                if latest['MA5'] > latest['MA20']:
                    score += 15
                if latest['MA20'] > latest['MA60']:
                    score += 15
            # RSI 打分
            if 30 <= latest['RSI'] <= 70:
                score += 20
            elif latest['RSI'] < 30:
                score += 15
            # MACD 打分
            if latest['MACD'] > latest['Signal']:
                score += 20
            # 成交量打分
            if latest['Volume_Ratio'] > 1.5:
                score += 30
            elif latest['Volume_Ratio'] > 1:
                score += 15

            # 附加 OBV 调整
            if latest['OBV'] > latest['OBV_MA10']:
                score += 5
            else:
                score -= 5

            # 附加随机指标调整
            if latest['%K'] < 20:
                score += 5
            elif latest['%K'] > 80:
                score -= 5

            return score

        except Exception as e:
            self.logger.error(f"计算打分失败：{str(e)}")
            raise

    @staticmethod
    def get_recommendation(score: float) -> str:
        """根据最终打分给出投资建议"""
        if score >= 80:
            return '强烈推荐买入'
        elif score >= 60:
            return '建议买入'
        elif score >= 40:
            return '建议观望'
        elif score >= 20:
            return '建议卖出'
        else:
            return '强烈建议卖出'

    def analyze_stock(self, stock_code: str) -> Dict:
        """针对单只股票执行完整的技术分析流程"""
        try:
            df = self.get_stock_data(stock_code)
            df = self.calculate_indicators(df)
            score = self.calculate_score(df)
            latest = df.iloc[-1]
            prev = df.iloc[-2]
            return {
                'stock_code': stock_code,
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'score': score,
                'price': latest['close'],
                'price_change': (latest['close'] - prev['close']) / prev['close'] * 100,
                'ma_trend': 'UP' if latest['MA5'] > latest['MA20'] else 'DOWN',
                'rsi': latest['RSI'],
                'macd_signal': 'BUY' if latest['MACD'] > latest['Signal'] else 'SELL',
                'volume_status': 'HIGH' if latest['Volume_Ratio'] > 1.5 else 'NORMAL',
                'recommendation': self.get_recommendation(score)
            }

        except Exception as e:
            self.logger.error(f"分析股票 {stock_code} 失败：{str(e)}")
            raise

# -------------------------------
# **全盘股票扫描器**
# -------------------------------
class TopStockScanner:
    """全盘筛选高打分股票的扫描器"""

    def __init__(self, max_workers: int = 20, min_score: float = 85):
        """
        初始化扫描器

        Args:
            max_workers: 并发线程数量（已增至20以加速分析）
            min_score: 高分最低阈值
        """
        self.analyzer = StockAnalyzer()
        self.max_workers = max_workers
        self.min_score = min_score
        self.logger = logging.getLogger(__name__)

    def get_all_stocks(self) -> List[str]:
        """
        获取所有上市 A 股股票代码（全盘版）。
        使用 ak.stock_info_sh_name_code(symbol="主板A股") 与 ak.stock_info_sz_name_code(symbol="A股列表")，
        候选字段列表为：['A股代码', '证券代码', '股票代码', 'code'] 。
        """
        try:
            sh_df = ak.stock_info_sh_name_code(symbol="主板A股")
            sz_df = ak.stock_info_sz_name_code(symbol="A股列表")
            candidate_cols = ['A股代码', '证券代码', '股票代码', 'code']

            def get_codes(df: pd.DataFrame) -> set:
                for col in candidate_cols:
                    if col in df.columns:
                        return {str(code).zfill(6) for code in df[col]}
                raise KeyError(f"未能找到股票代码字段，现有字段：{df.columns.tolist()}")

            sh_codes = get_codes(sh_df)
            sz_codes = get_codes(sz_df)
            all_codes = sorted(sh_codes | sz_codes)
            self.logger.info(f"完整股票列表获取到 {len(all_codes)} 支股票信息")
            print(f"\n开始分析 {len(all_codes)} 支股票...")
            return all_codes

        except Exception as e:
            self.logger.error(f"获取股票列表失败：{str(e)}")
            raise

    def analyze_stock_safe(self, stock_code: str, max_retries: int = 3) -> Optional[Dict]:
        """
        安全分析单只股票（加入重试机制），数据异常则跳过。
        """
        for attempt in range(max_retries):
            try:
                return self.analyzer.analyze_stock(stock_code)
            except ValueError as e:
                self.logger.warning(f"跳过股票 {stock_code}: {str(e)}")
                return None
            except Exception as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"股票 {stock_code} 分析尝试 {max_retries} 次后失败：{str(e)}")
                    return None
                self.logger.warning(f"股票 {stock_code} 第 {attempt+1} 次分析失败：{str(e)}")
                time.sleep(random.uniform(2, 5))

    def process_batch(self, stock_codes: List[str]) -> List[Dict]:
        """利用多线程并行处理一批股票的分析任务"""
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.analyze_stock_safe, code): code for code in stock_codes}
            for future in tqdm(futures, desc="分析进度", ncols=80):
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as e:
                    stock = futures[future]
                    self.logger.error(f"处理股票 {stock} 时出错：{str(e)}")
        return results

    def save_intermediate_results(self, results: List[Dict]) -> None:
        """周期性保存中间结果，便于后续查看进度"""
        try:
            df = pd.DataFrame(results)
            high_score_stocks = df[df['score'] >= self.min_score].sort_values('score', ascending=False)
            output_lines = [
                "=" * 80,
                f"股票扫描中间结果 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                f"共分析 {len(results)} 支股票",
                "=" * 80,
                f"\n发现 {len(high_score_stocks)} 支高分股票（得分≥{self.min_score}）："
            ]
            for _, row in high_score_stocks.iterrows():
                output_lines.extend([
                    f"\n股票代码: {row['stock_code']}",
                    f"得分: {row['score']:.1f} | 价格: ¥{row['price']:.2f} | 涨跌幅: {row['price_change']:.2f}%"
                ])

            os.makedirs('scanner', exist_ok=True)
            with open('scanner/temp_results.txt', 'w', encoding='utf-8') as f:
                f.write('\n'.join(output_lines))

        except Exception as e:
            self.logger.error(f"保存中间结果失败：{str(e)}")

    def get_high_score_stocks(self, batch_size: int = 20) -> List[Dict]:
        """扫描全盘股票，返回高打分结果列表"""
        try:
            all_stocks = self.get_all_stocks()
            total_stocks = len(all_stocks)
            print(f"\n开始扫描 {total_stocks} 支股票……")
            results = []
            total_batches = (total_stocks + batch_size - 1) // batch_size

            # for i in range(0, total_stocks, batch_size):
            #     batch_number = i // batch_size + 1
            #     print(f"\r当前进度: 批次 {batch_number}/{total_batches}", end="")
            #     batch = all_stocks[i:i + batch_size]
            #     batch_results = self.process_batch(batch)
            #     results.extend(batch_results)
            #     if i + batch_size < total_stocks:
            #         time.sleep(random.uniform(3, 5))
            #     if results and ((len(results) % 100 == 0) or (i + batch_size >= total_stocks)):
            #         self.save_intermediate_results(results)
            batch = all_stocks[0:1]
            batch_results = self.process_batch(batch)
            print("\n扫描结束！")

            if results:
                df_results = pd.DataFrame(results)
                high_score_stocks = df_results[df_results['score'] >= self.min_score].sort_values('score', ascending=False)
                formatted_results = []
                for _, row in high_score_stocks.iterrows():
                    formatted_results.append({
                        '股票代码': row['stock_code'],
                        '评分': f"{row['score']:.1f}",
                        '当前价格': f"¥{row['price']:.2f}",
                        '涨跌幅': f"{row['price_change']:.2f}%",
                        'RSI指标': f"{row['rsi']:.2f}",
                        '均线趋势': '上升' if row['ma_trend'] == 'UP' else '下降',
                        'MACD信号': '买入' if row['macd_signal'] == 'BUY' else '卖出',
                        '成交量状态': '放量' if row['volume_status'] == 'HIGH' else '正常',
                        '投资建议': row['recommendation']
                    })
                return formatted_results
            return []

        except Exception as e:
            self.logger.error(f"全盘扫描失败：{str(e)}")
            raise


# -------------------------------
# **结果分组与报告生成**
# -------------------------------
def format_price_category(price: float) -> str:
    """将价格划分为区间（例如 32.5 -> '30-40'）"""
    base = (price // 10) * 10
    return f"{int(base)}-{int(base+10)}"

def save_results_by_price(results: List[Dict]) -> None:
    """按价格区间保存分析结果至文件"""
    try:
        os.makedirs('scanner', exist_ok=True)
        price_groups = {}
        for stock in results:
            price = float(stock['当前价格'].replace('¥', ''))
            category = format_price_category(price)
            price_groups.setdefault(category, []).append(stock)

        for category, stocks in price_groups.items():
            output_lines = [
                "=" * 80,
                f"股票分析结果 - 价格区间: {category}元",
                f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "=" * 80,
                f"\n该区间共发现 {len(stocks)} 支高分股票（得分≥85）：",
                "-" * 80
            ]
            stocks.sort(key=lambda x: float(x['评分']), reverse=True)
            for i, stock in enumerate(stocks, 1):
                output_lines.extend([
                    f"\n{i}. 股票代码: {stock['股票代码']}",
                    f"   评分: {stock['评分']} | 价格: {stock['当前价格']} | 涨跌幅: {stock['涨跌幅']}",
                    f"   RSI指标: {stock['RSI指标']} | 均线趋势: {stock['均线趋势']} | MACD信号: {stock['MACD信号']}",
                    f"   成交量状态: {stock['成交量状态']}",
                    f"   投资建议: {stock['投资建议']}",
                    "-" * 80
                ])
            output_lines.extend([
                f"\n价格区间 {category}元 分析汇总：",
                f"1. 股票数量: {len(stocks)}",
                f"2. 平均评分: {np.mean([float(stock['评分']) for stock in stocks]):.1f}",
                f"3. 买入信号股票数: {sum(1 for stock in stocks if stock['MACD信号'] == '买入')}",
                f"4. 放量股票数: {sum(1 for stock in stocks if stock['成交量状态'] == '放量')}"
            ])

            filename = f'scanner/price_{category.replace("-", "_")}.txt'
            with open(filename, 'w', encoding='utf-8') as f:
                f.write('\n'.join(output_lines))
        create_summary_file(price_groups)
    except Exception as e:
        logging.error(f"保存结果时发生错误: {str(e)}")
        raise

def create_summary_file(price_groups: Dict[str, List[Dict]]) -> None:
    """生成综合汇总报告"""
    try:
        output_lines = [
            "=" * 80,
            "A股市场优质股票筛选报告",
            f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 80
        ]
        total_stocks = sum(len(stocks) for stocks in price_groups.values())
        all_scores = [float(stock['评分']) for stocks in price_groups.values() for stock in stocks]

        output_lines.extend([
            "\n整体统计：",
            f"1. 共筛选出 {total_stocks} 支高分股票（得分≥85）",
            f"2. 平均评分: {np.mean(all_scores):.1f}",
            f"3. 最高评分: {max(all_scores):.1f}",
            "\n各价格区间分布：",
            "-" * 80
        ])
        for category, stocks in sorted(price_groups.items(), key=lambda x: float(x[0].split('-')[0])):
            output_lines.extend([
                f"\n价格区间 {category}元：",
                f"  - 股票数量: {len(stocks)}",
                f"  - 平均评分: {np.mean([float(stock['评分']) for stock in stocks]):.1f}"
            ])

        with open('scanner/summary.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(output_lines))
    except Exception as e:
        logging.error(f"生成汇总报告失败：{str(e)}")
        raise

# -------------------------------
# **主程序入口**
# -------------------------------
def main():
    """程序主入口"""
    print("\n" + "=" * 80)
    print("Market-Wide High-Score Stock Scanner".center(76))
    print("=" * 80)

    scanner = TopStockScanner(max_workers=20)  # 已提升至20线程
    try:
        print("\n开始全盘扫描股票……")
        high_score_stocks = scanner.get_high_score_stocks(batch_size=20)
        if not high_score_stocks:
            print("\n未找到得分大于等于85分的股票。")
            return

        save_results_by_price(high_score_stocks)

        print(f"\n分析完成！结果已保存至 scanner 文件夹中：")
        print("1. 按价格区间保存的详细分析文件（price_XX_YY.txt）")
        print("2. 汇总报告（summary.txt）")

        temp_file = 'scanner/temp_results.txt'
        if os.path.exists(temp_file):
            os.remove(temp_file)

        print("\n" + "=" * 80)
        input("\n按Enter键退出……")

    except Exception as e:
        error_msg = f"\n程序错误：{str(e)}\n"
        print("=" * 80)
        print(error_msg)
        print("=" * 80)
        os.makedirs('scanner', exist_ok=True)
        with open('scanner/error_log.txt', 'w', encoding='utf-8') as f:
            f.write("Stock Analysis System Error Report\n")
            f.write("=" * 80 + "\n")
            f.write(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Error: {str(e)}\n")
            f.write("=" * 80 + "\n")
            f.write(f"详细堆栈信息:\n{traceback.format_exc()}")
        print("错误日志已保存至 scanner/error_log.txt")
        input("\n按Enter键退出……")

if __name__ == "__main__":
    main()
