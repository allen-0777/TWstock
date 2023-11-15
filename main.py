# -*- coding: utf-8 -*-
from flask import Flask,request,render_template, redirect, url_for, flash,send_from_directory,send_file, jsonify, session
from core.data import *
import json
import os
import time
import pandas as pd

app = Flask(__name__)
app.config['SECRET_KEY'] = '123'

@app.route('/')
def home():
    df = pd.read_csv('holding_list.csv',encoding='utf-8')
    url = "https://norway.twsthr.info/StockHolders.aspx?stock="
    df['stock'] = df['stock'].apply(lambda x: f'<a href="{url}{x}">{x}</a>')
    table = df.to_json(orient='records')
    df_three, data_date = three_data()
    three_table = df_three.to_json(orient='records')
    df_futures = futures()
    futures_table = df_futures
    return render_template('front_page.html', table=table,three_table=three_table,futures_table=futures_table)

@app.route('/for_ib_common')
def api_for_ib_common():
    df_common, data_date = for_ib_common()
    common_table = df_common.to_json(orient='records')
    return render_template('for_ib_common.html', common_table=common_table)

@app.route('/for_buy_sell')
def api_for_buy_sell():
    df_all, df_buy_top50, df_sell_top50, data_date = for_buy_sell()
    for_buy_table = df_buy_top50.to_json(orient='records')
    for_sell_table = df_sell_top50.to_json(orient='records')
    return render_template('for_buy_sell.html', all=df_all, for_buy_table=for_buy_table, for_sell_table=for_sell_table, date=data_date)

@app.route('/ib_buy_sell')
def api_ib_buy_sell():
    df_all, df_buy_top50, df_sell_top50, data_date = ib_buy_sell()
    ib_buy_table = df_buy_top50.to_json(orient='records')
    ib_sell_table = df_sell_top50.to_json(orient='records')
    return render_template('ib_buy_sell.html', all=df_all, ib_buy_table=ib_buy_table, ib_sell_table=ib_sell_table, date=data_date)

@app.route('/turnover')
def api_turnover():
    df = turnover()
    return render_template('turnover.html', data=df)

@app.route('/exchange_rate')
def api_exchange_rate():
    rate_data = exchange_rate()
    return render_template('exchange_rate.html', data=rate_data)

if __name__ == '__main__':
    app.run(port=5000, debug=True)



