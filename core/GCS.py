from google.cloud import bigquery, storage
import os, io, re
from pathlib import Path
from zipfile import ZipFile, is_zipfile
import pandas as pd
import numpy as np
import json
import logging
from time import sleep
from datetime import datetime, timedelta

from core.gcpfunc import CloudServiceInit

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s : %(levelname)s - %(message)s')

class FILEop:
    def AsiaToUTC(date):
        ''' 2023-01-01'''
        date = datetime.strptime(f"{date} 00:00:00", '%Y-%m-%d %H:%M:%S')
        # date = datetime.strptime(f"{date} 00:00:00", '%Y%m%d %H:%M:%S')

        return datetime.strftime(date - timedelta(hours=8),'%Y-%m-%d %H:%M:%S')

    def saveCsv(df, path, override=True):
        def _save():
            try:
                df.to_csv(path, encoding='utf-8-sig')
            except Exception as e:
                logging.info(f"FILEop_saveCsv: [{e}]")

        if override:
            _save()
        else:
           None if os.path.isfile(path) else _save()

    def readCsv(path):
        try:
            return pd.read_csv(path, index_col=0)
        except Exception as e:
            logging.info(f"FILEop_readCsv: [{e}]")

    def AbsPath():
        return Path().resolve()

    def readExcel(path, sheeet_name=None):
        try:
            return pd.read_excel(path, sheet_name=sheeet_name)
        except Exception as e:
            logging.info(f"FILEop_readExcel: [{e}]")

    def AbsPath():
        return Path().resolve()

    def Path():
        return Path()

    def createFolder(path):
        os.makedirs(path, exist_ok=True)

    def checkLog(path, file):
        # 檢查有無 pubsub 檔案, 否則下載檔案.
        def f(x, file):
            return x if re.findall(file,x) else None
        return list(filter(lambda x:f(x,file), os.listdir(path)))

    # def unZip(file_name, outfolder, password):
    #     if isinstance(file_name, str):
    #         try:
    #             with ZipFile(zip_io, "r") as f:
    #                 f.extractall(outfolder, pwd=password.encode())
    #         except Exception as e:
    #             print(e)

    #     elif isinstance(file_name, bytes):
    #         try:
    #             zip_io = io.BytesIO(file_name)
    #             with ZipFile(zip_io, "r") as f:
    #                 f.extractall(outfolder, pwd=password.encode())
    #         except Exception as e:
    #             print(e)

    def unZip(file_name, outfolder, password, reg=" "):
        if isinstance(file_name, (str, Path)):
            try:
                with ZipFile(f"{file_name}", "r") as zip_ref:
                    files = list(filter(lambda f: re.findall(reg, f), zip_ref.namelist()))
                    for file in files:
                        zip_ref.extract(file, outfolder, pwd=password.encode())
            except Exception as e:
                logging.error(f'upzip error file_name: {e}')

        elif isinstance(file_name, bytes):
            try:
                zip_io = io.BytesIO(file_name)
                with ZipFile(zip_io, "r") as zip_ref:
                    files = list(filter(lambda f: re.findall(reg, f), zip_ref.namelist()))
                    for file in files:
                        zip_ref.extract(file, outfolder, pwd=password.encode())
            except Exception as e:
                logging.error(f'upzip error file_name: {e}')

class GCP(CloudServiceInit):
    def __init__(self):
        super().__init__()
        self.FLOP = FILEop
        self.ETL_cols = GCP._ETL_cols()

    def gcsClient(self, bucket):
        try:
            if self.local:
                if bucket == "masterevbuscloud":
                    self.project = "busdata"
                    client = storage.Client.from_service_account_json(os.getenv("GOOGLE_APP_EVBUS"))
                elif bucket == "masterevbusphihongdatacloud":
                    self.project = "stationdata"
                    client = storage.Client.from_service_account_json(os.getenv("GOOGLE_APP_PHIHONG"))
            else:
                client = storage.Client.from_service_account_info(self.creds)
            masterBusclient = client.get_bucket(bucket)
            return masterBusclient
        except Exception as e:
            return e

    @staticmethod
    def _ETL_cols():
        return ['updateTime', 'filterCol', 'company',
                 'Plate', 'year', 'month', 'day',
                 'type','subLen', 'fileName', 'Path']

    def _BqDownLoad_PubSublog(self, date:str):
        utcTime = self.FLOP.AsiaToUTC(date)
        # 設定 BigQuery 客戶端
        if self.local:
            client = bigquery.Client.from_service_account_json(os.getenv("GOOGLE_APP_TRANSPORT"))
        else:
            self.project = "busdata"
            client = bigquery.Client()
        logging.info(f"query data time: {utcTime}")
        # 執行 SQL 查詢
        query = f"""
        DECLARE Time_8hr STRING;
        SET Time_8hr = FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S','{utcTime}');
        SELECT DISTINCT JSON_QUERY(data, '$.updated') AS updateTime,
        JSON_QUERY(data, '$.name') AS filePath,
        FROM `master-transportation.evbus._pubsub`
        WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, HOUR)
        BETWEEN TIMESTAMP(Time_8hr) AND TIMESTAMP_ADD(TIMESTAMP(Time_8hr),INTERVAL 24 HOUR)
        AND JSON_QUERY(data, '$.name') NOT LIKE '%tmp%'
        AND JSON_QUERY(data, '$.name') NOT LIKE '%test%'
        """
        query_job = client.query(query)
        result = query_job.result() 
        logging.info("Seccessfully query data")
        # 取回查詢結果
        return result.to_dataframe()

    def _pubsubfile(self, date:str):
        pubsubPath = FILEop.AbsPath() / "data" / "pubsub_log"
        FILEop.createFolder(pubsubPath)
        publist = FILEop.checkLog(pubsubPath, date)
        if len(publist) == 0:
            df = self._BqDownLoad_PubSublog(date)
            FILEop.saveCsv(df, f"{pubsubPath / date}.csv")
        else:
            df = FILEop.readCsv(f"{pubsubPath / date}.csv")
        return df

    @staticmethod
    def _lenFilter(df, pathlen:int, col='len'):
        tmp = df.loc[df[col] == pathlen].copy()
        tmp['Path'] = tmp['filePath'].str[1:-1]
        tmp_col = tmp['Path'].str.split("/", expand=True)
        tmp_col[['Path', 'updateTime']] = tmp[['Path', 'updateTime']]
        tmp_col = tmp_col.loc[tmp_col.ne('').all(axis=1)]
        if tmp_col.empty:
            tmp_col[0] = ""
        return tmp_col

    def _logcatFP(self, df, col='len'):
        tmp_col = GCP._lenFilter(df, 4)
        tmp_col = tmp_col.loc[tmp_col[0] =='logcat']

        def _df_logkk():
            if tmp_col.empty:
                return pd.DataFrame()
            else:
                df_logkk = pd.DataFrame(columns=self.ETL_cols)
                df_logkk['updateTime'] = pd.to_datetime(tmp_col['updateTime'].str[1:-1],
                                                    format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True)
                df_logkk['company'] = 'KK'
                df_logkk['Plate'] = tmp_col[1]
                df_logkk['year'] = tmp_col[2].str[0:4].astype(int)
                df_logkk['month'] = tmp_col[2].str[4:6].astype(int)
                df_logkk['day'] = tmp_col[2].str[6:8].astype(int)
                df_logkk['type'] = tmp_col[3].apply(lambda x: x.split('_')[0])
                df_logkk['subLen'] = tmp_col[3].apply(lambda x: x.split('.zip')[0][-2:])
                df_logkk['fileName'] = tmp_col[3]
                df_logkk['filterCol'] = "logcat"
                df_logkk['Path'] = tmp_col['Path'] 
                df_logkk = df_logkk.dropna()
                df_logkk = df_logkk[self.ETL_cols]
            return df_logkk

        def _df_newlogcat():
            tmp_col = GCP._lenFilter(df, 8)
            tmp_col = tmp_col.loc[tmp_col[0] == 'logcat']

            if tmp_col.empty:
                return pd.DataFrame()
            else:
                df_logPhy = pd.DataFrame(columns=self.ETL_cols)
                df_logPhy['updateTime'] = pd.to_datetime(tmp_col['updateTime'].str[1:-1], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True)
                df_logPhy['filterCol'] = tmp_col[0] 
                df_logPhy['company'] = tmp_col[1] 
                df_logPhy['Plate'] = tmp_col[2] 
                df_logPhy['year'] = tmp_col[3].astype(int)
                df_logPhy['month'] = tmp_col[4].astype(int)
                df_logPhy['day'] = tmp_col[5].astype(int) 
                df_logPhy['type'] = tmp_col[6] 
                df_logPhy['fileName'] = tmp_col[7]
                df_logPhy['subLen'] = tmp_col[7].apply(lambda x: x.split('.zip')[0][-4:]).astype(int)
                df_logPhy['Path'] = tmp_col['Path']
                df_logPhy = df_logPhy.dropna()
                df_logPhy = df_logPhy[self.ETL_cols]
            return df_logPhy

        # =========================================

        def _df_logPhy():
            tmp_col = GCP._lenFilter(df, 8)
            tmp_col = tmp_col.loc[tmp_col[0] == 'phy']

            if tmp_col.empty:
                return pd.DataFrame()
            else:
                df_logPhy = pd.DataFrame(columns=self.ETL_cols)
                df_logPhy['updateTime'] = pd.to_datetime(tmp_col['updateTime'].str[1:-1], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True)
                df_logPhy['filterCol'] = tmp_col[0] 
                df_logPhy['company'] = tmp_col[1] 
                df_logPhy['Plate'] = tmp_col[2] 
                df_logPhy['year'] = tmp_col[3].astype(int)
                df_logPhy['month'] = tmp_col[4].astype(int)
                df_logPhy['day'] = tmp_col[5].astype(int) 
                df_logPhy['type'] = tmp_col[6] 
                df_logPhy['fileName'] = tmp_col[7]
                df_logPhy['subLen'] = tmp_col[7].apply(lambda x: x.split('.zip')[0][-4:]).astype(int)
                df_logPhy['Path'] = tmp_col['Path']
                df_logPhy = df_logPhy.dropna()
                df_logPhy = df_logPhy[self.ETL_cols]
            return df_logPhy
        
        return pd.concat([_df_logkk(), _df_logPhy(), _df_newlogcat()], axis=0)

    def _phydataFP(self, df):
        tmp_col = GCP._lenFilter(df, 9)
        df_Phy_tmp = tmp_col.loc[tmp_col[0] != 'MasterEVBusPhiHongDataCloud']

        if df_Phy_tmp.empty:
            return pd.DataFrame()
        else:
            df_Phy = pd.DataFrame(columns=self.ETL_cols)
            df_Phy['updateTime'] = pd.to_datetime(df_Phy_tmp['updateTime'].str[1:-1], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True)
            df_Phy['filterCol'] = "phy"
            df_Phy['company'] = df_Phy_tmp[0]
            df_Phy['Plate'] = df_Phy_tmp[3]
            df_Phy['year'] = df_Phy_tmp[4].astype(int)
            df_Phy['month'] = df_Phy_tmp[5].astype(int)
            df_Phy['day'] = df_Phy_tmp[6].astype(int)
            df_Phy['type'] = df_Phy_tmp[7]
            df_Phy['subLen'] = df_Phy_tmp[8].apply(lambda x: x.split("_")[-2]).astype(int)
            df_Phy['fileName'] = df_Phy_tmp[8]
            df_Phy['Path'] = df_Phy_tmp['Path']
            df_Phy = df_Phy.dropna()
            df_Phy = df_Phy[self.ETL_cols]
        return df_Phy

    def _chgFP(self, df):
        def _df_chgKK_chia():
            tmp_col = GCP._lenFilter(df, 4)
            tmp_col = tmp_col.loc[tmp_col[0] == 'MasterEVBusPhiHongDataCloud']
            if tmp_col.empty:
                return pd.DataFrame()
            else:
                df_chgKK_chia = pd.DataFrame(columns=self.ETL_cols)
                df_chgKK_chia['updateTime'] = pd.to_datetime(tmp_col['updateTime'].str[1:-1], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True)
                df_chgKK_chia['company'] = 'KK_Chiayi'
                df_chgKK_chia['Plate'] = "NoPlate"
                df_chgKK_chia['year'] = tmp_col[2].str[0:4].astype(int)
                df_chgKK_chia['month'] = tmp_col[2].astype(int)
                df_chgKK_chia['day'] = tmp_col[3].apply(lambda x: x.split('.csv')[0][-2:]).astype(int)
                df_chgKK_chia['type'] = 'GB'
                df_chgKK_chia['subLen'] = 24
                df_chgKK_chia['fileName'] = tmp_col[3]
                df_chgKK_chia['filterCol'] = "PhiHong"
                df_chgKK_chia['Path'] = tmp_col['Path'] 
                df_chgKK_chia = df_chgKK_chia.dropna()
                df_chgKK_chia = df_chgKK_chia[self.ETL_cols]
            return df_chgKK_chia

        # =================================================

        def _df_chgKK_KWT():
            tmp_col = GCP._lenFilter(df, 6)
            tmp_col = tmp_col.loc[tmp_col[0] == 'MasterEVBusPhiHongDataCloud']
            if tmp_col.empty:
                return pd.DataFrame()
            else:
                df_chgKK_KWT = pd.DataFrame(columns=self.ETL_cols)
                df_chgKK_KWT['updateTime'] = pd.to_datetime(tmp_col['updateTime'].str[1:-1], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True)
                df_chgKK_KWT['filterCol'] = tmp_col[0].apply(lambda x: re.findall('PhiHong',x)[0])
                df_chgKK_KWT['company'] = tmp_col[3]
                df_chgKK_KWT['Plate'] = "NoPlate"
                df_chgKK_KWT['year'] = tmp_col[3].astype(int)
                df_chgKK_KWT['month'] = tmp_col[4].astype(int)
                df_chgKK_KWT['day'] = tmp_col[5].apply(lambda x: x.split("-")[-1][0:2]).astype(int)
                df_chgKK_KWT['type'] = tmp_col[1] + "_" + tmp_col[2] 
                df_chgKK_KWT['subLen'] = 24
                df_chgKK_KWT['fileName'] = tmp_col[5]
                df_chgKK_KWT['Path'] = tmp_col['Path']
                df_chgKK_KWT = df_chgKK_KWT.dropna()
                df_chgKK_KWT = df_chgKK_KWT[self.ETL_cols]
            return df_chgKK_KWT 

        # =================================================

        def _df_chgPhy():
            tmp_col = GCP._lenFilter(df, 9)
            tmp_col = tmp_col.loc[tmp_col[0] == 'MasterEVBusPhiHongDataCloud']
            if tmp_col.empty:
                return pd.DataFrame()
            else:
                df_chgPhy = pd.DataFrame(columns=self.ETL_cols)
                df_chgPhy['updateTime'] = pd.to_datetime(tmp_col['updateTime'].str[1:-1], format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True)
                df_chgPhy['filterCol'] = tmp_col[0].apply(lambda x: re.findall('PhiHong',x)[0])
                df_chgPhy['company'] = tmp_col[3]
                df_chgPhy['Plate'] = tmp_col[5]
                df_chgPhy['year'] = tmp_col[6].astype(int)
                df_chgPhy['month'] = tmp_col[7].astype(int)
                df_chgPhy['day'] = tmp_col[8].apply(lambda x: x.split("-")[-1][0:2]).astype(int)
                df_chgPhy['type'] = tmp_col[1] + "_" + tmp_col[2] 
                df_chgPhy['subLen'] = 24
                df_chgPhy['fileName'] = tmp_col[8]
                df_chgPhy['Path'] = tmp_col['Path']
                df_chgPhy = df_chgPhy.dropna()
                df_chgPhy = df_chgPhy[self.ETL_cols]
            return df_chgPhy

        return pd.concat([_df_chgKK_chia(), _df_chgKK_KWT(), _df_chgPhy()], axis=0)


    def gcsPath(self, bucket, uri):
        client = self.gcsClient(bucket)

        if re.match(bucket, uri):  # 判斷路徑若包含 bucket 就去除
            uri = uri[5:].split('/', 1)[-1]
        blobs_tmp = list(client.list_blobs(prefix=uri))

        # 提取 updateTime 與 filePath
        # 過濾檔案的 blob info
        blobs = list(filter(lambda x: re.findall("zip|csv", x.name), blobs_tmp))
        dirs ={"updateTime":[], 'filePath':[]}

        # 提取 blob -> updateTime, filePath
        def _x(obj):
            dirs.get("updateTime").append(obj.time_created)
            dirs.get("filePath").append(f'"{obj.name}"') # 加上" " 配合 pubsub一致格式
        [*map(lambda obj: _x(obj) ,blobs)]
        
        # 轉 df, 並判斷是否為空白 df
        df = pd.DataFrame(dirs) 
        if df.empty: # 是，則回傳空 df
            return df

        # updateTime 轉換格式, 並加上" "符號, 配合 pubsub一致格式
        df['updateTime'] = df['updateTime'].dt.strftime('"%Y-%m-%dT%H:%M:%S.%fZ"')
        df['len'] = df['filePath'].apply(lambda x: len(x.split("/")))
        df = pd.concat([self._logcatFP(df), self._phydataFP(df), self._chgFP(df)])
        return df.reset_index(drop='index')

    def etlPath(self, date:str):
        '''讀取原始log -> ETL -> 合併 df'''
        df = self._pubsubfile(date)
        df['len'] = df['filePath'].apply(lambda x: len(x.split("/")))
        df = pd.concat([self._logcatFP(df), self._phydataFP(df), self._chgFP(df)])
        # df['updateTime'] = df['updateTime'].dt.to_period('s').copy()
        return df.reset_index(drop='index')

    def filterPath(self, date:str, Plate:str, subLen:list, dataType:str='logcat'):
        ''' date: "2023-01-01" \n
        Plate: "EAL0001"\n
        subLen: [07,08,09]\n
        dataType: (default:'logcat')
        '''
        df = self.etlPath(date)
        subDate = date.split("-")
        def eqValue(df, col, var):
            return df.loc[df[col] == var]
        def eqList(df, col, li):
            return df.loc[df[col].isin(li)]

        if len(subDate) == 3: 
            df = eqValue(df, "Plate", Plate)
            df = eqValue(df, "type", dataType)
            df = eqValue(df, "month", float(subDate[1]))
            df = eqValue(df, "day", float(subDate[2]))
            df = eqList(df, "subLen", subLen)
            # if dataType == "logcat":
            #     df = eqValue(df, Plate, logcat)
            if df.empty:
                logging.info(f"查詢無資料 [{df}]")
            return df
        else:
            logging.info(f"date 格式錯誤 [{date}], try: 2023-01-01")
            return pd.DataFrame()

    def loadFiles(self, df, select_upzip_file=" ", keep_zip=False, __max=4000):
        '''*** 輸入 ETLpath df 下載檔案 ***\n
        google project:\n
        1. charge station data ['masterevbusphihongdatacloud']\n
        2. (logcat, LTE, NEMA), (phy, raw) data ['masterevbuscloud']\n
        '''
        if len(df) > __max:
            return "超過下載檔案數量限制"
        if len(df) == 0:
            return "無檔案可下載"

        # ==========================================
        Chg_df = df.loc[df['filterCol'] == 'PhiHong']
        evbus_df = df.loc[df['filterCol'] != 'PhiHong']
        df_objlist = [Chg_df, evbus_df]

        def _objDf_downloal(df, select_upzip_file):
            reg = select_upzip_file
            if df.empty:
                return None

            if any(df['filterCol'].isin(['PhiHong'])):
                bucket = 'masterevbusphihongdatacloud'
            else:
                bucket = 'masterevbuscloud'
            gcsClient = self.gcsClient(bucket)       
            local_path = FILEop.AbsPath() / 'data' / 'GCPfile'
            FILEop.createFolder(local_path)

            for i, row in df.iterrows():
                if re.findall('.zip',row['Path']):
                    folder_path = local_path / row['company'] / row['Plate'] / row['type']
                    FILEop.createFolder(folder_path)
                    file_existed = FILEop.checkLog(folder_path, row['fileName'].replace(".zip",""))

                    # Check if there is a file, if found, skip this loop.
                    if len(file_existed) == 1:
                        logging.info(f"Existed file: {file_existed}")
                        continue
                    FILEop.unZip(gcsClient.blob(row['Path']).download_as_bytes(), folder_path, "1359", reg=reg)
                    logging.info(f"Seccessfully download and upzip: {row['fileName']}")

                else:  # no unzip
                    folder_path = local_path / row['company'] / row['Plate'] / row['type']
                    FILEop.createFolder(folder_path)
                    file_existed = FILEop.checkLog(folder_path, row['fileName'].replace(".zip",""))

                    if len(file_existed) == 1:
                        logging.info(f"Existed file: {file_existed}")
                        continue
                    gcsClient.blob(row['Path']).download_to_filename(folder_path / f"chg_{row['company']}_{row['Plate']}_{row['fileName']}")
                    logging.info(f"Seccessfully download file: chg_{row['company']}_{row['Plate']}_{row['fileName']}")
        
        def _objDf_downloal_zip(df, select_upzip_file): 
            '''For keep zip files use'''
            reg = select_upzip_file  #篩選檔案(正規表達式)
            if df.empty:
                return None

            if any(df['filterCol'].isin(['PhiHong'])):
                bucket = 'masterevbusphihongdatacloud'
            else:
                bucket = 'masterevbuscloud'

            gcsClient = self.gcsClient(bucket)       
            local_path = FILEop.AbsPath() / 'data' / 'GCPfile'
            FILEop.createFolder(local_path)

            for i, row in df.iterrows():
                if re.findall('.zip',row['Path']):
                    folder_path = local_path / row['company'] / row['Plate'] / row['type'] / str(row['month']).zfill(2)
                    folder_path_zip = folder_path / "zip"
                    FILEop.createFolder(folder_path_zip)

                    # Check if there is a file, if not found then go to GCP download the file
                    file_existed = FILEop.checkLog(folder_path_zip, row['fileName'])
                    if len(file_existed) == 0: 
                        # logging.info(f"Existed file: {file_existed}")
                        gcsClient.blob(row['Path']).download_to_filename(folder_path_zip / row['fileName'])

                    # upzip file
                    FILEop.unZip(file_name=folder_path_zip / row['fileName'], outfolder=folder_path, password="1359", reg=reg)
                    # logging.info(f"Seccessfully download save zip and upzip file: {row['fileName']}")
                else:  # no upzip
                    folder_path = local_path / row['company'] / row['Plate'] / row['type']
                    FILEop.createFolder(folder_path)
                    file_existed = FILEop.checkLog(folder_path, row['fileName'].replace(".zip",""))

                    if len(file_existed) == 1:
                        logging.info(f"Existed file: {file_existed}")
                        continue
                    gcsClient.blob(row['Path']).download_to_filename(folder_path / f"chg_{row['company']}_{row['Plate']}_{row['fileName']}")
                    logging.info(f"Seccessfully download file: chg_{row['company']}_{row['Plate']}_{row['fileName']}")

        if keep_zip:
            [*map(lambda df: _objDf_downloal_zip(df, select_upzip_file), df_objlist)]
        else:
            [*map(lambda df: _objDf_downloal(df), df_objlist)]

        tmp = df.groupby(["Plate","month", "day"], as_index=False).first()[["Plate","month", "day"]]
        info = tmp.apply(lambda r: "_".join(r.astype(str)), axis=1).to_list()
        return f"Seccessfully download save zip and upzip files: {info}"

class LOADbq(CloudServiceInit):
    def __init__(self):
        super().__init__()
        self.FLOP = FILEop

    def _bqLoadPhydata(self, date:str):
        ''' input date format: 2023-01-01 '''
        # 設定 BigQuery 客戶端
        if self.local:
            client = bigquery.Client.from_service_account_json(os.getenv("GOOGLE_APP_TRANSPORT"))
        else:
            self.project = "busdata"
            client = bigquery.Client()

        query = f'''
        SELECT Time, Company, SrvArea, BusModel, BusPlate, ActDCCur, ActDCVolt
        FROM `master-transportation.evbus.battery_phy`
        WHERE DATE(Time) = "{date}";
        '''
        try:
            query_job = client.query(query)
            while query_job.running(): 
                sleep(0.2)

            result = query_job.result() 
            logging.info("Seccessfully query data")
        except Exception as e:
            logging.info(f"failed query data [ {e} ]")
            return pd.DataFrame()

        return result.to_dataframe()

