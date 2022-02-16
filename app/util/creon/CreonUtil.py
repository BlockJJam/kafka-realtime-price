import json
import win32com.client
from datetime import date, datetime
import time

from app.config import LoggerConfig
from app.kafka.KafkaProducer import KafkaProducer
from app.kafka.KafkaAdmin import KafkaAdmin
from app.util.model.stock.OrderedPrice import *
from app.util.creon.CreonDispatchMapper import *

g_logger = LoggerConfig.get_logger()

"""
 CreonAPI Subscribe시에 활성화되는 Event
 Author: jaemin.joo
"""
class CreonPlusEvent:
    kafka_producer = KafkaProducer()
    today = date.today().strftime('%Y%m%d')

    def set_params(self, client, name, caller):
        self.external_api = client
        self.name = name
        self.caller = caller


    def OnReceived(self):
        '''
        external_api로 들어오는 실시간 시세를 데이터로 정형화하고 kafka producer 인스턴스로 데이터를 밀어넣는 메서드
        Author: jaemin.joo
        '''
        code, timess, exFlag, cprice, diff, cVol, vol = self.__extract_price_by_event_instance()

        if (exFlag == ord('1')):  # 동시호가 시간 (예상체결)
            g_logger.debug(f"{code} - 실시간(예상체결) {timess} * {cprice} 대비 {diff} 체결량 {cVol} 거래량 {vol}" )
        elif (exFlag == ord('2')):  # 장중(체결)
            g_logger.debug(f"{code} - 실시간(장중체결) {timess} {cprice} 대비 {diff} 체결량 {cVol} 거래량 {vol}" )
            data = OrderedPrice(code, self.today + timess, cprice, diff, cVol, vol)

            g_logger.debug("-------------------------")
            g_logger.debug(f"{data.make_json_str()}")

            # @TODO 쓸데없는 의존성 추출 필요
            self.kafka_producer.push(code, data.make_json_str(), datetime.today().strftime("%Y%m%d"))
            self.kafka_producer.flush()

    def __extract_price_by_event_instance(self):
        # time = CpEvent.instance.GetHeaderValue(3)  # 시간
        code = self.external_api.GetHeaderValue(0)
        timess = str(self.external_api.GetHeaderValue(18))  # 초
        exFlag = self.external_api.GetHeaderValue(19)  # 예상체결 플래그
        cprice = self.external_api.GetHeaderValue(13)  # 현재가
        diff = self.external_api.GetHeaderValue(2)  # 대비
        cVol = self.external_api.GetHeaderValue(17)  # 순간체결수량
        vol = self.external_api.GetHeaderValue(9)  # 거래량
        
        return code,timess,exFlag,cprice,diff,cVol,vol


''' 
CreonAPI 시세에 대한 데이터를 요청하고 받아오는 세션을 핸들링하는 클래스 
'''
class CreonPlusStockMQ:
    def __init__(self, name):
        self.stock_realtime_external_api = CreonDispatchMapper().get_stock_realtime_external_api()
        self.creon_status_external_api = CreonDispatchMapper().get_creon_staus_external_api()
        self.name = name

    def Subscribe(self, code, caller):
        '''
        실시간 시세를 받아왔을 때 핸들링할 이벤트를 등록하고, Creon API를 구독(세션연결)하는 메서드
        Author: jaemin.joo
        '''
        self.stock_realtime_external_api.SetInputValue(0, code)

        handler = win32com.client.WithEvents(self.stock_realtime_external_api, CreonPlusEvent)
        handler.set_params(self.stock_realtime_external_api, self.name, caller)

        self.__check_is_send_transaction_and_wait(2)
        self.stock_realtime_external_api.Subscribe()

    def Unsubscribe(self):
        self.stock_realtime_external_api.Unsubscribe()

    def __check_is_send_transaction_and_wait(self, type):
        """
        Subscribe 제한 개수까지 남은 개수를 통해 요청 제한을 피하기 위한 메서드 TR 요청 제한을 피하기 위한 메서드. 제한에 걸릴 것 같으면 n초간 대기
        Author: jaemin.joo
        """
        remain_count = self.creon_status_external_api.GetLimitRemainCount(type)
        g_logger.info(f"실시간 시세 Subscribe를 할 수 있는 남은 개수: {remain_count}")
        if remain_count == 00:
            g_logger.info(f'실시간 시세를 대기하여 남은 개수가 없습니다. {self.creon_status_external_api.LimitRequestRemainTime / 1000} 초간 대기하겠습니다. ')
            time.sleep(self.creon_status_external_api.LimitRequestRemainTime / 1000)


'''
CreonAPI를 활용한 비즈니스 로직을 핸들링하는 클래스
Author: jaemin.joo
'''
class CreonPlusStockManager:
    def __init__(self):
        self.creon_status_external_api = CreonDispatchMapper().get_creon_staus_external_api()
        self.stock_master_external_api = CreonDispatchMapper().get_stock_master_external_api()
        self.stockcode_external_api = CreonDispatchMapper().get_stockcode_external_api()
        self.stock_MQ_dict = dict()
        self.kafka_admin = KafkaAdmin()

    def Request(self, code):
        '''
        해당 종목에 대한 일반적인 정보와 통신 상태를 체크하는 메서드
        Author: jaemin.joo
        '''
        
        # 연결 여부 체크
        is_connect_creon = self.creon_status_external_api.IsConnect
        if (is_connect_creon == 0):
            g_logger.debug("PLUS가 정상적으로 연결되지 않음. ")
            return False

        # 현재가 객체 구하기
        self.stock_master_external_api.SetInputValue(0, code)
        self.stock_master_external_api.BlockRequest()

        # 현재가 통신 및 통신 에러 처리
        master_req_status = self.stock_master_external_api.GetDibStatus()
        status_msg = self.stock_master_external_api.GetDibMsg1()
        g_logger.debug(f"통신상태 {master_req_status} {status_msg}")

        if master_req_status != 0:
            return False

        code, name, time, cprice, diff, open, high, low, offer, bid, vol, vol_value = self.__extract_curr_priceinfo(self.stock_master_external_api)

        g_logger.debug("코드 이름 시간 현재가 대비 시가 고가 저가 매도호가 매수호가 거래량 거래대금")
        g_logger.debug(f"{code}, {name}, {time}, {cprice}, {diff}, {open}, {high}, {low}, {offer}, {bid}, {vol}, {vol_value}")
        return True

    def __extract_curr_priceinfo(self, stock_master_external_api):
        '''
        실시세 데이터 정보에서 필요한 데이터를 추출하는 메서드
        Author: jaemin.joo
        '''
        code = stock_master_external_api.GetHeaderValue(0)  # 종목코드
        name = stock_master_external_api.GetHeaderValue(1)  # 종목명
        time = stock_master_external_api.GetHeaderValue(4)  # 시간
        cprice = stock_master_external_api.GetHeaderValue(11)  # 종가
        diff = stock_master_external_api.GetHeaderValue(12)  # 대비
        open = stock_master_external_api.GetHeaderValue(13)  # 시가
        high = stock_master_external_api.GetHeaderValue(14)  # 고가
        low = stock_master_external_api.GetHeaderValue(15)  # 저가
        offer = stock_master_external_api.GetHeaderValue(16)  # 매도호가
        bid = stock_master_external_api.GetHeaderValue(17)  # 매수호가
        vol = stock_master_external_api.GetHeaderValue(18)  # 거래량
        vol_value = stock_master_external_api.GetHeaderValue(19)  # 거래대금

        return code,name,time,cprice,diff,open,high,low,offer,bid,vol,vol_value

    def get_all_symbols_names(self):
        """
        크레온에서 조회가능한 모든 상품의 [종목코드]를 가져오는 메서드
        Author: jaemin.joo
        """
        symbols_KOSPI = self.stockcode_external_api.GetStockListByMarket(1)
        symbols_KOSDAQ = self.stockcode_external_api.GetStockListByMarket(2)
        #g_logger.debug(f"symbol info: {symbols_KOSPI}")
        # (symbol, name, status) 로 구성된 튜플들을 원소로 가진 리스트
        symbols_and_names = [symbol for symbol in symbols_KOSPI + symbols_KOSDAQ]
        
        return symbols_and_names

    def get_symbol_codes_by_cnt(self, cnt=10):
        """
        크레온에서 조회가능한 모든 상품 중에 일부 [종목코드]를 가져오는 메서드
        Author: jaemin.joo
        """
        if type(cnt) is not int:
            raise TypeError

        try:
            symbols_KOSPI = self.stockcode_external_api.GetStockListByMarket(1)                     
            symbols_KOSDAQ = self.stockcode_external_api.GetStockListByMarket(2)
            #g_logger.debug(f"symbol info: {symbols_KOSPI}")
            # (symbol, name, status) 로 구성된 튜플들을 원소로 가진 리스트
            symbol_codes = [symbol for symbol in symbols_KOSPI + symbols_KOSDAQ]    
        except Exception as e:
            g_logger.error(f"symbol 호출 error: {e}0")
        
        return symbol_codes[:cnt]

    def stop_subscribe(self):
        '''
        subscribe가 진행중인 Stock MQ 인스턴스를 unsubscribe로 세션을 끊어주는 메서드
        Author: jaemin.joo
        '''
        g_logger.info(f"{len(self.stock_MQ_dict)}개의 stock_MQ를 Unscribe!")
        for code, stock_MQ in self.stock_MQ_dict.items():
            g_logger.debug(f"{code}'s instance Unscribe!")
            stock_MQ.Unsubscribe()

    def request_realtime_price(self, code_list):
        '''
        실시세를 받아오기를 요청하는 메서드
        - kafka 종목 별 토픽을 미리 생성하는 요청 포함
        Author: jaemin.joo
        '''
        # kafka에 code에 해당하는 topic이 없으면 생성한다
        self.kafka_admin.create_topics(code_list)
        # self.kafka_admin.delete_topics(code_list)

        for code in code_list:    
            if (self.Request(code) == False):
                continue
            
            g_logger.debug(f"실시간 현재가 요청 code: {code}")
            self.stock_MQ_dict[code] = CreonPlusStockMQ("stockcur")
            self.stock_MQ_dict[code].Subscribe(code, self)
        return len(self.stock_MQ_dict)