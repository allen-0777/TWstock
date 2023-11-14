import re
import pandas as pd
import os

def get_process_list(lines, process_steps):
    process_list = []
    for line in lines:
        for step in process_steps:
            if step in line:
                time = re.search(r'\d{2}:\d{2}:\d{2}', line)
                if time:
                    process_list.append([time.group(), step])
    return process_list

def get_matches(lines, pattern):
    matches = []
    for line in lines:
        match = pattern.search(line)
        if match:
            matches.append(' '.join(match.group(1, 2, 3, 4, 5, 6, 7)))
    return matches

def get_TTIA_lines(lines, pattern):
    TTIA_lines = []
    for line in lines:
        match = pattern.search(line)
        if match:
            TTIA_lines.append([match.group(1)[:8], f"{match.group(2)} [id, sequence]: {match.group(3)}, {match.group(4)}"])
    return TTIA_lines

def process_file(file_path):
    process_steps = [
        "自動選擇路線成功",
        "自動確認方向成功 - 往程",
        "自動切換方向成功 - 返程",
        "自動結班成功"
    ]

    with open(file_path, 'r') as file:
        lines = file.readlines()

    process_list = get_process_list(lines, process_steps)
    df_process = pd.DataFrame(process_list, columns=['Time', 'Info'])
    df_process['Time'] = pd.to_datetime(df_process['Time'])
    df_process['Time'] = df_process['Time'].dt.strftime('%H:%M:%S')

    pattern = re.compile(r"(\d{2}:\d{2}:\d{2}).*?" + r"(motcstate:.*?) .*?" + r"(dutystate:.*?) .*?" + r"(Multi_Card_dir:.*?) .*?" + r"(MOTC_dir:.*?) .*?" + r"(key:.*?) .*?" + r"(pincode:.*?) .*")
    matches = get_matches(lines, pattern)
    df = pd.DataFrame(matches, columns=['Full_Info'])
    df['Time'] = df['Full_Info'].str.split(" ", expand=True)[0]
    df['Info'] = df['Full_Info'].str.replace(r"\d{2}:\d{2}:\d{2} ", "")
    df = df[['Time','Info']]

    pattern = re.compile(r"\[(\d{2}:\d{2}:\d{2}:\d{4})\]\[TTIADataReporter.1.0.29.4994.Connector\] (TTIA server send|TTIA server response) \[id, sequence\]: (\d{1,4}), (\d{1,4})")
    TTIA_lines = get_TTIA_lines(lines, pattern)
    df_TTIA = pd.DataFrame(TTIA_lines, columns=['Time', 'Info'])
    df_TTIA['Time'] = pd.to_datetime(df_TTIA['Time'])
    df_TTIA['Time'] = df_TTIA['Time'].dt.strftime('%H:%M:%S')

    df_com = pd.concat([df_process, df, df_TTIA]).sort_values('Time')

    df_com['Abnormal'] = df_com['Info'].apply(lambda x: 'pincode異常' if 'pincode:' in x and x.split('pincode:')[1].strip() == '' else '')

    df_com = df_com[['Time', 'Info', 'Abnormal']]
    return df_com

def process_excel_file(file_path):
    filename = os.path.basename(file_path)
    parts = filename.split('_')
    fil_date = parts[1]
    
    df = pd.read_excel(file_path, engine='openpyxl')
    fil_df = df[df['回傳筆數是否均超過合格率'] == '否'].copy()
    fil_df['遺漏時間'] = (fil_df['遺漏資料數'] / 12).round().astype(int)
    result = fil_df[['觸發時間', '方向','車牌','遺漏資料數','遺漏時間', '回傳筆數是否均超過合格率']]
    fil_df['觸發時間'] = pd.to_datetime(fil_df['觸發時間'])
    fil_plate = fil_df['車牌'].str.replace('-','').iloc[0]
    fil_hour = fil_df['觸發時間'].dt.hour.iloc[0]
    fil_hour = str(fil_hour).zfill(2)
    
    return result, fil_date, fil_hour, fil_plate

    




