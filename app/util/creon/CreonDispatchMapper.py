import win32com.client

'''
CreonAPI의 기능별 api를 Naming에 맞게 매핑한 Instance를 제공하는 Mapper 클래스
Author: jaemin.joo
'''
class CreonDispatchMapper:
    def __init__(self) :
        self.stock_realtime_external_api = win32com.client.Dispatch("DsCbo1.StockCur")
        self.stock_master_external_api = win32com.client.Dispatch("DsCbo1.StockMst")
        self.creon_status_external_api = win32com.client.Dispatch("CpUtil.CpCybos")
        self.stockcode_external_api = win32com.client.Dispatch('CpUtil.CpCodeMgr')

    def get_stock_realtime_external_api(self):
        return self.stock_realtime_external_api
        
    def get_stock_master_external_api(self):
        return self.stock_master_external_api

    def get_creon_staus_external_api(self):
        return self.creon_status_external_api

    def get_stockcode_external_api(self):
        return self.stockcode_external_api

        