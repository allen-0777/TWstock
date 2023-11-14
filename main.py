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
    df = pd.read_csv('list.csv',encoding='utf-8')
    url = "https://norway.twsthr.info/StockHolders.aspx?stock="
    df['stock'] = df['stock'].apply(lambda x: f'<a href="{url}{x}">{x}</a>')
    # html_table = df.to_html(classes='display', index=False, border=0, escape=False)
    table = df.to_json(orient='records')
    df_three, data_date = three_data()
    three_table = df_three.to_json(orient='records')
    df_futures = futures()
    # futures_table = df_futures.to_json(orient='records')
    futures_table = df_futures
    return render_template('front_page.html', table=table,three_table=three_table,futures_table=futures_table)

@app.route('/turnover')
def api_turnover():
    df = turnover()
    return render_template('turnover.html', data=df)

@app.route('/for_buy_sell')
def api_for_buy_sell():
    df_all, df_buy_top50, df_sell_top50, data_date = for_buy_sell()
    return render_template('for_buy_sell.html', all=df_all, buy_top50=df_buy_top50, sell_top50=df_sell_top50, date=data_date)

@app.route('/ib_buy_sell')
def api_ib_buy_sell():
    df_all, df_buy_top50, df_sell_top50, data_date = ib_buy_sell()
    return render_template('ib_buy_sell.html', all=df_all, buy_top50=df_buy_top50, sell_top50=df_sell_top50, date=data_date)

@app.route('/for_ib_common')
def api_for_ib_common():
    df_common, data_date = for_ib_common()
    return render_template('for_ib_common.html', common=df_common, date=data_date)

@app.route('/exchange_rate')
def api_exchange_rate():
    rate_data = exchange_rate()
    return render_template('exchange_rate.html', data=rate_data)

# @app.route('/',methods=['GET'])
# def home():
#     return render_template('front_page.html')

@app.route('/',methods=['GET'])
@app.route('/<string:company_name>', methods=['GET', 'POST'])
def display_company(company_name=None):
    date = request.form.get('date', "2023-09-02")
    print('Selected date', date)
    
    df_com = VIEWPlot.listPull(date)
    plates = df_com.get(company_name, [])
    dataplot_obj = DataPlot(date)
    df = dataplot_obj._phydataLoad()
    filterHour_dir = OPdata.filterHour(df)
    data_dir = eval(filterHour_dir)
    plates_data = []
    for plate in plates:
        hours_links = []
        for hour in data_dir[company_name].get(plate, []):
            link_hour = url_for('query_hour', date=date, plate=plate ,hour=hour)
            link_html_hour = f'<a href="{link_hour}" class="hour-link">{hour}</a>'
            hours_links.append(link_html_hour)

        link_plate = url_for('query_plate',date=date, plate=plate)
        link_html_plate = f'<a href="{link_plate}" class="plate-link">{plate}</a>'
        plates_data.append({
            "車牌號碼": link_html_plate,
            "hours": hours_links
        })

    if request.method == 'POST':
        return jsonify(plates_data)
    return render_template('front_page.html', company=company_name, plates_data=plates_data)

# @app.route('/',methods=['GET'])
# @app.route('/shinbus',methods=['GET','POST'])
# def display_shinbus():
#     date = request.form.get('date',"2023-09-02")
#     print('Selected date', date)
    
#     df_com = VIEWPlot.listPull(date)
#     company = "shinbus-Taipei"
#     plates = df_com.get('shinbus-Taipei', [])
#     dataplot_obj = DataPlot(date)
#     df = dataplot_obj._phydataLoad()
#     filterHour_dir = OPdata.filterHour(df)
#     data_dir = eval(filterHour_dir)
    
#     plates_data = []
#     for plate in plates:
#         hours = data_dir[company].get(plate, [])
#         link = url_for('query_plate',date=date, plate=plate)
#         link_html = f'<a href="{link}" class="plate-link">{plate}</a>'
#         plates_data.append({
#             "車牌號碼": link_html,
#             "hours": hours
#         })
#     if request.method == 'POST':
#         return jsonify(plates_data)
#     return render_template('company.html', company=company, plates_data=plates_data)

@app.route('/query-plate/<date>/<plate>', methods=['GET'])
@app.route('/query-plate/<date>/<plate>/<hour>', methods=['GET'])
def query_plate(plate,date,hour=None):
    result = VIEWPlot.betweenPlot(date, plate)
    df = pd.DataFrame(result)
    df['Time'] = pd.to_datetime(df['Time'])
    df.set_index('Time', inplace=True)
    hourly_avg = df[['ActDCCur','ActDCVolt']].resample('H').mean()
    
    return hourly_avg.reset_index().to_json(orient="records", date_format='iso')

@app.route('/query-hour/<date>/<plate>/<hour>', methods=['GET'])
def query_hour(date, plate, hour):
    view_intervalTB = VIEWPlot.intervalTB(date, plate, hour)
    print(view_intervalTB)
    return view_intervalTB.reset_index().to_json(orient="records", date_format='iso')

@app.route('/upload', methods=['GET','POST'])
def upload():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        f = request.files['file']
        if f.filename == '':
            flash('No selected file')
            return redirect(request.url)
            
        upload_file_path = FILEop.Path() / 'data'/'uploadFile'
        FILEop.createFolder(upload_file_path)
        f.save(upload_file_path / f.filename)

        mag = Data.saveUpFileCSV(Analyze.busSchedule(f.filename), f.filename.replace('xlsx', 'csv'))
        logging.info(mag)
        
        return redirect(url_for('read_file', file_name=f.filename))
    else:
        return render_template('upload.html')

@app.route('/coming', methods=['GET','POST'])
def coming():
    return render_template('coming_data.html')

@app.route('/analyze', methods=['GET','POST'])
def analyze():
    return render_template('analyze.html')

@app.route('/read_file/<path:file_name>', methods=['GET'])
def read_file(file_name):
    bus_view = VIEW.busSchedule(file_name)
    return render_template('schedule.html', table=bus_view)

@app.route('/results/<file_name>/<bus_no>', methods=['GET'])
def results(file_name, bus_no):
    table = VIEW.logcat(file_name, bus_no)

    return render_template('results.html',table=table)

@app.route('/submit', methods=['POST'])
def submit():
    date = request.form.get('date')
    plate = request.form.get('plate')
    sublen = request.form.get('sublen')  
    # print(sublen)
    # if sublen: 
        # sublen = str(sublen)
    #     sublen = sublen.split(',')
        # if sublen == 3: 
            # sublen.zfill(4)
    # else:
    #     sublen = []
    table = VIEW.filterLogcat(date, plate, [sublen])
    return render_template('results.html', table=table)

@app.route('/download', methods=['GET'])
def download():
    
    return render_template('download.html')

if __name__ == '__main__':
    app.run(port=5000, debug=True)



