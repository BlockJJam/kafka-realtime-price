import ctypes
from importlib.resources import path
import os
import time
import sys, pathlib
sys.path.append(str(pathlib.Path().absolute()))

from pywinauto import application
from win32com.client import Dispatch
from config import LoggerConfig
from config import ConfigParserConfig
from brokerage.BrokerageConnector import *

g_logger = LoggerConfig.get_logger()
g_config = ConfigParserConfig.get_configparser()


class CreonConnector(BrokerageConnector):
    
    def __init__(self):
        # app
        self.app = None

        # 사용할 TR
        self.cpCodeMgr = None
        self.stockChart = None
        self.cpStatus = Dispatch('CpUtil.CpCybos')

    def __del__(self):
        g_logger.debug("Creon Plus가 종료됩니다.")
        if self.app:
            self.app.kill()

    def check_and_wait(self, type):
        """
        조회 TR 요청 제한을 피하기 위한 메서드. 제한에 걸릴 것 같으면 n초간 대기
        :return:
        """
        remain_count = self.cpStatus.GetLimitRemainCount(type)
        if remain_count <= 10:
            g_logger.debug(f'시세/주문 연속 조회 제한 회피를 위해 {self.cpStatus.LimitRequestRemainTime / 1000} 초간 대기하겠습니다. ')
            time.sleep(self.cpStatus.LimitRequestRemainTime / 1000)

    # 크레온 API 접속 코드
    def connect(self):
        """
        CREON_PLUS 에 접속하는 메서드
        :return:
        """
        # 기존 연결 여부 확인
        cp_cybos = Dispatch("CpUtil.CpCybos")

        if Dispatch("CpUtil.CpCybos").IsConnect:
            # 기존 연결 있는 경우, 생략
            g_logger.error("이미 Creon Plus 에 연결되어 있습니다")
            return

        # 관리자 권한 실행 확인
        if self.__not_exec_process_by_admin() :
            return

        # Creon 자동 실행 및 로그인 + 로그인 대기
        self.__exec_Creon_process_for_5m()

        # 현재 프로세스에서 Windows환경의 프로그램 구성을 활용할 수 있도록 만드는 기능
        self.app = application.Application()

        # CREON 로그인 정보( CREON, Daeshin 둘 중 하나만 주석처리를 해제하고 사용할것!!!!)
        userid = g_config['CREON_USER']['ID']
        userpw = g_config['CREON_USER']['PWD']
        certpw = g_config['CREON_USER']['CERTPWD']
        coStarter_path = g_config['CREON_EXE']['PATH']
        self.app.start(f'{coStarter_path} /prj:cp /id:{userid} /pwd:{userpw} /pwdcert:{certpw} /autostart')

        # 연결 여부 체크
        if self.__is_connected_Creon_api():
            g_logger.debug(f"Creon Plus 연결 성공")
        else:
            g_logger.error("Creon Plus 연결에 실패했습니다")

    # Creon API 연결 여부 method
    def __is_connected_Creon_api(self):
        timeout = 0
        g_logger.debug("Creon Plus 연결중...")

        while Dispatch("CpUtil.CpCybos").IsConnect == 0:
            time.sleep(1)
            timeout += 1
            # 100초가 넘도록 Creon이 실행되지 않았을 경우 오류로 판단합니다.

            if timeout > 100:
                g_logger.error("Creon 연결에 문제가 생겼습니다.")
                g_logger.error("인터넷 연결 상태 등을 확인 바랍니다.")
                return False
        return True

    def __exec_Creon_process_for_5m(self):
        os.system('taskkill /IM coStarter* /F /T')
        os.system('taskkill /IM CpStart* /F /T')
        os.system('wmic process where "name like \'%coStarter%\'" call terminate')
        os.system('wmic process where "name like \'%CpStart%\'" call terminate')
        g_logger.debug("Creon 실행중...")

        time.sleep(5)

    def __not_exec_process_by_admin(self):
        if ctypes.windll.shell32.IsUserAnAdmin():
            g_logger.debug("관리자 권한으로 실행된 프로세스입니다.")
            return False
        else:
            g_logger.error("일반권한으로 실행됨. 관리자 권한으로 실행해 주세요.")
            return True
