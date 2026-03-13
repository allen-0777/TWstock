# -*- coding: utf-8 -*-
from flask import Flask, render_template, abort
from core.data import (
    three_data, futures, exchange_rate,
    for_ib_common, for_buy_sell, ib_buy_sell, turnover,
)
import pandas as pd

app = Flask(__name__)


@app.route('/')
def home():
    df_three, data_date = three_data()
    three_table = df_three.to_json(orient='records') if df_three is not None else 'null'

    df_futures = futures()
    if isinstance(df_futures, pd.DataFrame):
        futures_table = df_futures.reset_index().to_json(orient='records')
    else:
        futures_table = 'null'

    ex_rate = exchange_rate()
    if ex_rate is not None:
        ex_rate_table = ex_rate.reset_index().to_json(orient='records')
    else:
        ex_rate_table = 'null'

    return render_template(
        'front_page.html',
        three_table=three_table,
        futures_table=futures_table,
        ex_rate_table=ex_rate_table,
        data_date=data_date,
    )


@app.route('/for_ib_common')
def api_for_ib_common():
    df_common, data_date = for_ib_common()
    if df_common is None:
        abort(503, description="資料暫時無法取得，請稍後再試")
    return render_template(
        'for_ib_common.html',
        common_table=df_common.to_json(orient='records'),
        data_date=data_date,
    )


@app.route('/for_buy_sell')
def api_for_buy_sell():
    df_all, df_buy_top50, df_sell_top50, data_date = for_buy_sell()
    if df_buy_top50 is None:
        abort(503, description="資料暫時無法取得，請稍後再試")
    return render_template(
        'for_buy_sell.html',
        for_buy_table=df_buy_top50.to_json(orient='records'),
        for_sell_table=df_sell_top50.to_json(orient='records'),
        data_date=data_date,
    )


@app.route('/ib_buy_sell')
def api_ib_buy_sell():
    df_all, df_buy_top50, df_sell_top50, data_date = ib_buy_sell()
    if df_buy_top50 is None:
        abort(503, description="資料暫時無法取得，請稍後再試")
    return render_template(
        'ib_buy_sell.html',
        ib_buy_table=df_buy_top50.to_json(orient='records'),
        ib_sell_table=df_sell_top50.to_json(orient='records'),
        data_date=data_date,
    )


@app.route('/turnover')
def api_turnover():
    df = turnover()
    if df is None:
        abort(503, description="資料暫時無法取得，請稍後再試")
    return render_template('turnover.html', data=df)


@app.route('/exchange_rate')
def api_exchange_rate():
    rate_data = exchange_rate()
    if rate_data is None:
        abort(503, description="資料暫時無法取得，請稍後再試")
    return render_template('exchange_rate.html', data=rate_data)


if __name__ == '__main__':
    app.run(port=5000, debug=True)
