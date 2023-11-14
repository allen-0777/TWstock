from core.GCS import *
from core.read_excel import *
import core.CONF as CONF

class DataPlot(LOADbq):
    phydata_path = FILEop.Path()/ 'data' / 'phydata'

    def __init__(self, date):
        super().__init__()
        FILEop.createFolder(DataPlot.phydata_path)
        self.date = date

    # def _createPhydir():
    #     path = DataPlot.phydata_path
    #     FILEop.createFolder(str(path))

    def _phydataLoad(self):
        date = self.date
        file_name =  f"ActDCCur-Volt_{date}.csv"
        path = DataPlot.phydata_path / file_name

        if path.exists():
            phy_df = FILEop.readCsv(path)
            logging.info("phydata file exists!")
        else:
            phy_df = self._bqLoadPhydata(date)
            FILEop.saveCsv(phy_df, path)
            logging.info("phydata file saved path: data/phydata")
        
        return phy_df

class VIEWPlot:
    def listPull(date):
        obj_dp = DataPlot(date)
        return OPdata.filterCpSvrArea(obj_dp._phydataLoad())
    
    def listPull_2(date):
        obj_dp = DataPlot(date)
        return OPdata.filterHour(obj_dp._phydataLoad())

    def timePlot(plate):
        pass

    def betweenPlot(date, plate):
        obj_dp = DataPlot(date)
        return OPdata.prePlateOp(obj_dp._phydataLoad(), 'Time', plate)

    def intervalTB(date, plate, hour):
        obj_dp = DataPlot(date)
        return OPdata.prePlateOp_time(obj_dp._phydataLoad(), 'Time', plate, hour)





class OPdata:    
    def filterCpSvrArea(df, cols=['Company', 'SrvArea', 'BusPlate']):
        if len({'Company', 'SrvArea', 'BusPlate'} & set(df.columns)) != 3:
            return df
        tmp_df = df.groupby(['Company', 'SrvArea', 'BusPlate'], as_index=False).first()
        tmp_df['index_cpSa'] = tmp_df.apply(lambda row: f"{row['Company']}-{row['SrvArea']}", axis=1)
        tmp_df = tmp_df[['index_cpSa','BusPlate']]
        tmp_df = tmp_df.groupby('index_cpSa').apply(lambda row: row['BusPlate'].to_list())
        return tmp_df.to_dict()

    def filterHour(df, cols=['Time', 'Company', 'SrvArea', 'BusPlate']):
        if len({'Company', 'SrvArea', 'BusPlate'} & set(df.columns)) != 3:
            return df
        tmp_df = df[cols].copy()
        tmp_df['Time'] = pd.to_datetime(tmp_df['Time']).dt.strftime("%H")
        plate_hr = pd.DataFrame(tmp_df.groupby('BusPlate').apply(lambda row: row['Time'].sort_values().unique()))

        tmp_df2 = tmp_df.groupby(['Company', 'SrvArea', 'BusPlate'], as_index=False).first()
        tmp_df2['index_cpSa'] = tmp_df2.apply(lambda row: f"{row['Company']}-{row['SrvArea']}", axis=1)
        mapping_df = plate_hr.join(tmp_df2[['index_cpSa','BusPlate']].set_index('BusPlate')).reset_index()

        result_info = mapping_df.groupby('index_cpSa').apply(lambda r: r[[0, 'BusPlate']].set_index('BusPlate', drop=True).to_dict()[0])
        return result_info.to_json(orient="index")
    
    def prePlateOp(df, time_col, plate, cols='BusPlate'):
        plate = plate if isinstance(plate, list) else [plate]
        try:
            df[time_col] = pd.to_datetime(df[time_col])
            df = df.loc[df[cols].isin(plate)].sort_values(time_col)
        except Exception as e:
            print(f" {e}")
        return df

    def prePlateOp_time(df, time_col, plate, time, cols='BusPlate'):
        plate = plate if isinstance(plate, list) else [plate]
        try:
            df[time_col] = pd.to_datetime(df[time_col])
            df = df.loc[df[cols].isin(plate)].sort_values(time_col)
            df = df.loc[df[time_col].dt.strftime("%H") == time]
        except Exception as e:
            print(f" {e}")
        return df



