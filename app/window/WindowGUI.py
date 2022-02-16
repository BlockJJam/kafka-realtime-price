from PyQt5.QtWidgets import *
from app.config.LoggerConfig import  *
from util.creon.CreonUtil import *

g_logger = get_logger()

class WindowGUI(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("CREON PLUS API TEST")
        self.setGeometry(300, 300, 300, 150)
        self.stock_manager = CreonPlusStockManager()
        self.stock_MQ_dict = dict()

        btn1 = QPushButton("요청 시작", self)
        btn1.move(20, 20)
        btn1.clicked.connect(self.btn1_clicked)

        btn2 = QPushButton("요청 종료", self)
        btn2.move(20, 70)
        btn2.clicked.connect(self.btn2_clicked)

        btn3 = QPushButton("종료", self)
        btn3.move(20, 120)
        btn3.clicked.connect(self.btn3_clicked)

    def btn1_clicked(self):
        
        code_list = self.stock_manager.get_symbol_codes_by_cnt(100)
        exec_stockMQ_cnt = self.stock_manager.request_realtime_price(code_list)

        g_logger.info(f"{exec_stockMQ_cnt}개의 stock MQ 인스턴스에서 실시간 현재가 요청 시작!")
        self.is_rq = True

    def btn2_clicked(self):
        self.stock_manager.stop_subscribe()
        g_logger.info("실시간 현재가 요청 멈춤!")

    def btn3_clicked(self):
        self.stock_manager.stop_subscribe()
        g_logger.info("실시간 현재가 요청 멈춤 및 프로그램 종료!")
        exit()