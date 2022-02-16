import json

'''
체결 데이터 모델 클래스
Author: jaemin.joo
'''
class OrderedPrice:
    def __init__(self, code, timess, cprice, diff, cVol, vol):
        self.code = code
        self.timess = timess
        self.cprice = cprice
        self.diff = diff
        self.cVol = cVol
        self.vol = vol

    def make_json_str(self):
        return json.dumps(self.__dict__)