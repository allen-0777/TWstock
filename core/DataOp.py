from distutils.command import upload
from core.GCS import *
from core.read_excel import *
import core.CONF as CONF

class Data(GCP):
    gcpfile_path = FILEop.Path() / 'data'/ 'GCPfile'
    uploal_path = FILEop.Path() / 'data' / 'uploadFile'
    def __init__(self, date):
        super().__init__()
        self.date = date
        self.logcat_df = self._pbsLocat()

    def _pbsLocat(self):
        day = self.dateToDt(self.date).day
        pubsub_df = self.etlPath(self.date)
        pubsub_df = pubsub_df.loc[pubsub_df['type'].isin(['logcat', 'LOGCAT'])]
        pubsub_df = pubsub_df.loc[pubsub_df['day'] == day]
        return pubsub_df

    def subToFiles(self, plate:str, subLi:list, includeMimax=False):
        if isinstance(subLi, (list, np.ndarray)):
            subLi = list(map(lambda x: x if isinstance(x, int) else int(x) ,subLi))
        else:
            subLi = [subLi] if isinstance(subLi, int) else [int(subLi)]

        df = self.logcat_df
        filter_df = df.loc[df['Plate'] == plate]
        if includeMimax:
            filter_df = filter_df.loc[(min(subLi)*100-1 < df['subLen'].astype(int)) &\
                                      (max(subLi)*100+1 > df['subLen'].astype(int))]
        else:
            filter_df = filter_df.loc[df['subLen'].isin(subLi)]
        return filter_df

    def dateToDt(self, date):
        return datetime.strptime(f"{date} 00:00:00", '%Y-%m-%d %H:%M:%S')

    def readUpFile(file_name):
        FILEop.createFolder(Data.uploal_path)
        return FILEop.readExcel(Data.uploal_path/file_name, sheeet_name=0)

    def saveUpFileCSV(df, file_name):
        if re.search(".csv", file_name) == []:
            return "file name not .csv"
        view_path = Data.uploal_path / 'viewTB'
        FILEop.createFolder(view_path)
        FILEop.saveCsv(df, view_path / file_name)
        return f"Successed save {file_name}"
    
    def readTxt(file_path):
        try:
            return pd.read_fwf(file_path, header=None)
        except Exception as e:
            print(f"readTxt errro [ {e} ]")
            return pd.DataFrame()

    def dataFilePath(df, refileNm = "txt"):
        if df.empty:
            return list
        paths_list = df.apply(lambda row:\
                "/".join([str(Data.gcpfile_path),
                          row['company'],
                          row['Plate'],
                          row['type'],
                          row['fileName'].replace("zip",refileNm)]), axis=1).to_list()
        return paths_list               

    def _concatTxt(df):
        result = pd.DataFrame()
        df = df.sort_values('subLen')
        sub_info = df['subLen'].to_list()
        sub_info = ["no", "file"] if sub_info == [] else sub_info

        # plate = df['Plate'].values()[0]
        plate = np.unique(df['Plate'].values)[0]

        for path in Data.dataFilePath(df, "txt"):
            df = Data.readTxt(path)
            result = pd.concat([result, df])

        path = Path(path)
        # result_path = root_path / path.parent / f"logcat_{plate}_{sub_info}.csv"
        result_path = path.parent / f"logcat_{plate}_{sub_info[0]}-{sub_info[-1]}.csv"
        FILEop.saveCsv(result.reset_index(drop=True), result_path, override=True)
        return result_path

class VIEW:
    def busSchedule(file_name):
        file_path = Data.uploal_path / "viewTB" / file_name.replace("xlsx", "csv")
        
        bus_sd = FILEop.readCsv(file_path)
        bus_sd = bus_sd.drop(columns=['logcat'])

        bus_sd['GCP資料'] = bus_sd.apply(
            lambda row: f'<a href="/results/{file_name}/{row["班次"]}" style="color: light-blue; text-decoration: none;">logcat</a>', axis=1)
        bus_sd['分析結果'] = bus_sd.apply(lambda row: f'<select class="form-control"><option value="微星車機">微星車機</option value="寶錄車機"><option>寶錄車機</option><option value="駕駛操作程序">駕駛操作程序</option></select>', axis=1)
        # bus_sd['分析結果'] = bus_sd.apply(lambda row: f'<select class="form-control" name="analysis_result_{{ item['班次'] }}"><option value="微星車機">微星車機</option value="寶錄車機"><option>寶錄車機</option><option value="駕駛操作程序">駕駛操作程序</option></select>', axis=1)
        table = bus_sd.to_dict('records')
        return table

    def logcat(file_name, bus_no):
        file_path = Data.uploal_path / "viewTB" / file_name.replace("xlsx", "csv")
        bus_sd = FILEop.readCsv(file_path)
        logcat_path = bus_sd.loc[bus_sd['班次'] == int(bus_no)]['logcat'].values[0]
        print("!!!!!!!!!")
        table = Analyze._etlTxt(FILEop.readCsv(logcat_path))
        table = table.rename(columns={0:"Time", 2:"Info", 3:"Num"})
        print(table)
        
        # ============== Analyze.def()============================
        table['Time'] = table['Time'].apply(lambda time_str: ':'.join(time_str.split(':')[:3]))
        state = Analyze.mergeString(table,'Num', ['TTIA', '自動', 'state']) 
        if state.empty:
            state = Analyze.mergeString(table,'Info', ['TTIA', '自動', 'state'])
        state['Status'] = state['Info'].apply(lambda x: 'pincode異常' if 'pincode:' in x and x.split('pincode:')[1].strip() == '' else '')
        table = state.sort_values(by='Time')
        # ============== Analyze============================
        table = table[['Time', 'Info', 'Num', 'Status']]
        table = table.astype(str).to_dict('records')
        return table

    def filterLogcat(date, Plate, subLen):
        data_obj = Data(date)
        filter_df = data_obj.subToFiles(Plate, subLen)
        data_obj.loadFiles(filter_df)
        print(Data.dataFilePath(filter_df)[0])
        
        logcat_df = Data.readTxt(Data.dataFilePath(filter_df)[0])
        if logcat_df.empty:
            print("Info: Not found file")
            return {"Info":"Not found file"}

        logcat_df[0] = logcat_df[0].str.replace("\[", "", regex=True) # change this
        logcat_df = logcat_df[0].str.split("]", expand=True)
        logcat_df = logcat_df.rename(columns={0:"Time", 1:"Info", 2:"Num"})

        # ============== Analyze.def()============================
        logcat_df['Time'] = logcat_df['Time'].apply(lambda time_str: ':'.join(time_str.split(':')[:3]))
        state = Analyze.mergeString(logcat_df, "Num", ['TTIA', '自動', 'state'])
        if state.empty:
            state = Analyze.mergeString(logcat_df, "Info", ['TTIA', '自動', 'state'])
        state['Status'] = logcat_df['Info'].apply(lambda x: 'pincode異常' if 'pincode:' in x and x.split('pincode:')[1].strip() == '' else '')
        # ============== Analyze============================
        print("state!!!!!!!")
        print(state)
        if state.empty:
            logcat_df["Status"] = ""
            table = logcat_df.copy()

        table = state.sort_values(by='Time')
        table = table[['Time', 'Info', 'Num', 'Status']]
        table = table.astype(str).to_dict('records')
        return table

class Analyze:
    
    @staticmethod
    def _etlTxt(df):
        df["0"] = df["0"].str.replace("\[", "", regex=True) # change this
        df = df["0"].str.split("]", expand=True)
        return df

    @staticmethod
    def _processTxt(bus_sd):
        date = np.unique(bus_sd['date'].values)[0]
        data_obj = Data(date)

        bus_sd_down = bus_sd[['車牌', 'trig_hour']].groupby('車牌').sum()
        bus_sd_down['trig_hour'] = bus_sd_down['trig_hour'].apply(lambda x: np.unique(x))
        for plate, row in bus_sd_down.iterrows():
            donwDf = data_obj.subToFiles(plate, row['trig_hour'], includeMimax=True)
            data_obj.loadFiles(donwDf)

        for i, row in bus_sd.iterrows():
            df = data_obj.subToFiles(row["車牌"], row['trig_hour'], includeMimax=True)
            result_path = Data._concatTxt(df)
            bus_sd.loc[i, 'logcat'] = result_path 
        return bus_sd
            
    def busSchedule(file_name):
        def _calcList(li:list):
            result = []
            li.sort()
            for i in li:
                i = int(i)
                result.append([str(i-1).zfill(2), str(i).zfill(2), str(i+1).zfill(2)])
            return result

        df = Data.readUpFile(file_name)
        date = re.findall(r"\d{4}-\d{2}-\d{2}", file_name)
        bus_sd = df[df['回傳筆數是否均超過合格率'] == '否'].copy()
        if bus_sd.empty:
            return df
        bus_sd["date"] = date[0] if len(date) != 0 else None
        bus_sd["車牌"] = bus_sd["車牌"].str.replace("-","")
        bus_sd['遺漏時間'] = (bus_sd['遺漏資料數'] / 12).round().astype(int)
        bus_sd['trig_hour'] = _calcList(bus_sd['觸發時間'].apply(lambda x: x.split(':')[0]).to_list())
        bus_sd = Analyze._processTxt(bus_sd)

        return bus_sd

# ======================================================

    def mergeString(logcat_df, col:str, str_li:list):
        result_df = pd.DataFrame()
        if col not in logcat_df.columns:
            return result_df
        for s in str_li:
            tmp_df = logcat_df[logcat_df[col].str.contains(s, na=False)]
            result_df = pd.concat([result_df, tmp_df])
        return result_df

    def pincodeErr(logcat_df, col:str, str_li:list):
        """ 確認有無 pincode 表可以核對，司機是否有登錄"""
        pass
# ======================================================
    def get_process_list(lines, process_steps):
        process_list = []
        for line in lines:
            for step in process_steps:
                if step in line:
                    time = re.search(r'(\d{2}:\d{2}:\d{2})', line)
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