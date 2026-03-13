# -*- coding: utf-8 -*-
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from bs4 import BeautifulSoup
from cachetools import TTLCache, cached

# 快取 1 小時（台股盤後資料每日只更新一次）
_cache = TTLCache(maxsize=20, ttl=3600)


def parse_numeric_column(df, col):
    """移除千分位逗號並轉為數值型別。"""
    df[col] = df[col].str.replace(',', '')
    df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


@cached(_cache)
def get_data(max_attempts=5):
    now = datetime.now()
    ts = int(time.time() * 1000)
    for _ in range(max_attempts):
        datestr = now.strftime("%Y%m%d")
        url = (
            f"https://www.twse.com.tw/rwd/zh/fund/T86"
            f"?date={datestr}&selectType=ALL&response=json&_={ts}"
        )
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
        except Exception as e:
            print(f"get_data request failed: {e}")
            now -= timedelta(days=1)
            time.sleep(1)
            continue
        if data.get("total", 1) == 0:
            print(f"No data for date {datestr}, trying the previous day.")
            now -= timedelta(days=1)
            time.sleep(1)
            continue
        df = pd.DataFrame(data["data"], columns=data["fields"])
        return df, now.strftime("%Y-%m-%d")
    return None, None


@cached(_cache)
def three_data():
    ts = int(time.time() * 1000)
    try:
        response = requests.get(
            f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?response=json&_={ts}",
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"three_data failed: {e}")
        return None, None

    data_date = data["date"]
    df = pd.DataFrame(data["data"], columns=data["fields"])
    df['買賣差額'] = df['買賣差額'].astype(str)
    df['買賣差'] = df['買賣差額'].apply(format_number)
    return df[['單位名稱', '買賣差']].copy(), data_date


@cached(_cache)
def turnover():
    now = datetime.now()
    datestr = now.strftime("%Y%m%d")
    ts = int(time.time() * 1000)
    url = (
        f"https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK"
        f"?date={datestr}&response=json&_={ts}"
    )
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"turnover failed: {e}")
        return None

    df = pd.DataFrame(data["data"], columns=data["fields"])
    df['成交金額'] = df['成交金額'].astype(str)
    df['成交量'] = df['成交金額'].apply(format_number)
    return df[['日期', '成交量', '漲跌點數']].copy()


def for_buy_sell():
    try:
        df, data_date = get_data()
        if df is None or df.empty:
            return None, None, None, None
        col = '外陸資買賣超股數(不含外資自營商)'
        df_for = df[['證券代號', '證券名稱', col]].copy()
        df_for = parse_numeric_column(df_for, col)
        df_for.rename(columns={col: '外資買賣超股數'}, inplace=True)
        df_for_all = df_for[df_for['外資買賣超股數'].notna() & (df_for['外資買賣超股數'] != 0)]
        df_for_all = df_for_all.sort_values('外資買賣超股數', ascending=False)
        return df_for_all, df_for_all.head(50), df_for_all.tail(50), data_date
    except Exception as e:
        print(f"for_buy_sell failed: {e}")
        return None, None, None, None


def ib_buy_sell():
    try:
        df, data_date = get_data()
        if df is None or df.empty:
            return None, None, None, None
        col = '投信買賣超股數'
        df_ib = df[['證券代號', '證券名稱', col]].copy()
        df_ib = parse_numeric_column(df_ib, col)
        df_ib_all = df_ib[df_ib[col].notna() & (df_ib[col] != 0)]
        df_ib_all = df_ib_all.sort_values(col, ascending=False)
        return df_ib_all, df_ib_all.head(50), df_ib_all.tail(50), data_date
    except Exception as e:
        print(f"ib_buy_sell failed: {e}")
        return None, None, None, None


@cached(_cache)
def for_ib_common():
    try:
        # get_data() 已快取，for_buy_sell / ib_buy_sell 共用同一份資料
        df_for_all, _, _, data_date = for_buy_sell()
        df_ib_all, _, _, _ = ib_buy_sell()
        if df_for_all is None or df_ib_all is None:
            return None, None
        df_com_buy = pd.merge(df_for_all, df_ib_all, on='證券代號')
        df_com_buy.drop('證券名稱_y', axis=1, inplace=True)
        df_com_buy.rename(columns={'證券名稱_x': '證券名稱'}, inplace=True)
        df_com_buy = df_com_buy[
            (df_com_buy['外資買賣超股數'] >= 0) & (df_com_buy['投信買賣超股數'] >= 0)
        ]
        df_com_buy = df_com_buy.sort_values(by='投信買賣超股數', ascending=False)
        return df_com_buy, data_date
    except Exception as e:
        print(f"for_ib_common failed: {e}")
        return None, None


@cached(_cache)
def exchange_rate():
    try:
        # 直接取近 6 個月歷史匯率，不需先爬首頁
        resp = requests.get("https://rate.bot.com.tw/xrt/quote/l6m/USD", timeout=10)
        resp.encoding = 'utf-8'
        history = BeautifulSoup(resp.text, "lxml")
        history_table = (
            history.find(name='table', attrs={'title': '歷史本行營業時間牌告匯率'})
            .find(name='tbody')
            .find_all(name='tr')
        )
    except Exception as e:
        print(f"exchange_rate failed: {e}")
        return None

    date_history, history_buy, history_sell = [], [], []
    for row in history_table:
        if row.a is None:
            continue
        date_str = datetime.strptime(row.a.get_text(), '%Y/%m/%d').strftime('%Y/%m/%d')
        date_history.append(date_str)
        rates = row.find_all(
            name='td',
            attrs={'class': 'rate-content-cash text-right print_table-cell'},
        )
        history_buy.append(float(rates[0].get_text()))
        history_sell.append(float(rates[1].get_text()))

    df = pd.DataFrame({'date': date_history, 'buy_rate': history_buy, 'sell_rate': history_sell})
    return df.set_index('date').sort_index(ascending=True)


@cached(_cache)
def futures():
    try:
        tables = pd.read_html('https://www.taifex.com.tw/cht/3/futContractsDate')
        df = tables[2]
        df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
        headers = [
            "序號", "商品名稱", "身份別",
            "多方口數", "多方交易契約金額",
            "空方口數", "空方契約金額",
            "多空淨額口數", "多空淨額契約金額",
            "多方未平倉口數", "多方未平倉契約金額",
            "空方未平倉口數", "空方未平倉契約金額",
            "多空淨額未平倉口數", "多空淨額未平倉契約金額",
        ]
        df.columns = headers
        df = df[5:].reset_index(drop=True)
        df = df[~df['序號'].str.contains('期貨合計|期貨小計|序 號', na=False)]
        df = df[df['商品名稱'] == '臺股期貨']
        df = df.loc[:, ["多空淨額未平倉口數", "多空淨額未平倉契約金額"]]
        df.index = ["自營商", "投信", "外資"]
        return df
    except Exception as e:
        print(f"futures failed: {e}")
        return None


def format_number(num_str):
    num_float = float(num_str.replace(",", ""))
    return round(num_float / 1e8, 1)
