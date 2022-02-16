from threading import Timer
from app.config import LoggerConfig
import time

logger = LoggerConfig.get_logger()

'''
인수로 들어오는 function을 정확한 주기로 반복 동작하는 클래스
Author: jaemin.joo
'''
class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs) -> None:
        self._timer = None
        self.interval = interval
        self.next_call = time.time()
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.result = None
        self.start()


    def _run(self):
        '''
        Timer에 비동기적으로 동작하는 스레드 run 함수로, 주기적으로 실행될 function을 돌리고 결과를 출력하는 메서드
        Author: jaemin.joo
        '''
        self.is_running = False
        self.start()

        # 현재 Kafka Consumer가
        self.result = self.function(*self.args, **self.kwargs)
        if self.result is not None:
            if not self.result.error():
                logger.debug(f"offset: {self.result.offset()}, Received message: {self.result.value().decode('utf-8')}")
            else:
                logger.debug(f'Error occured: {self.result.error().str()}')

    def start(self):
        '''
        timer를 생성하여, 반복적으로 run메서드를 실행시키는 메서드
        Author: jaemin.joo
        '''
        if not self.is_running:
            self.next_call += self.interval
            self._timer = Timer(self.next_call - time.time() , self._run)
            self._timer.start()
            self.is_running = True
        
    def join(self):
        '''
        현재 사용하지 않음! timer의 start메서드로 return되는 결과물을 확인하기 위한 메서드, _run 메서드에서 해당 기능을 대신
        Author: jaemin.joo
        '''
        self._timer.join()
        return self.result

    def stop(self):
        '''
        타이머를 종료시키는 메서드
        Author: jaemin.joo  
        '''
        self._timer.cancel()
        self.is_running = False