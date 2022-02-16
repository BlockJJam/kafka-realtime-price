import sys
import win32com.client
import json
from brokerage.creon.CreonConnector import *
from datetime import datetime, date

from window.WindowGUI import *
from config import LoggerConfig

import sys
import warnings

g_logger = LoggerConfig.get_logger()

if __name__ == "__main__":
    # 밑에 2줄은 pywinauto 와 PyQt5를 함께 사용할 때 생기는 문제점
    warnings.simplefilter("ignore", UserWarning)
    sys.coinit_flags = 2
    
    creon_api = CreonConnector()
    creon_api.connect()

    app = QApplication(sys.argv)
    creonWindow = WindowGUI()
    creonWindow.show()

    app.exec_()