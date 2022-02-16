# Kafka 연동을 위한 Python module import.
# pip install confluent_kafka 명령을 통해 해당 모듈을 import 해야 함

from confluent_kafka import Producer

from config import LoggerConfig
from config import ConfigParserConfig


'''
topic: 종목 code
partitions: topic 당 1개
keyvalue: 순서보장을 위한 것이긴 하나 필요가 없을 수도
Author: jaemin.joo
'''
class KafkaProducer:
    def __init__(self):
        # kafka 서버 주소
        self.config_mapper = ConfigParserConfig.get_configparser()
        self.logger = LoggerConfig.get_logger()

        self.vBootstrap_server = self.config_mapper['KAFKA_SERVER']['URL']
        self.__connect_kafka_server()

    def __connect_kafka_server(self):
        # kafka 서버 연결
        try:
            self.producer = Producer({'bootstrap.servers': self.vBootstrap_server})
        except Exception as e:
            self.logger.info("Kafka Connect Fail")

    def __del__(self):
        self.producer.flush()

    def push(self, topic, data, keyvalue=''):
        """
        해당 topic 으로 data 를 add 하는 메서드
        Author: jaemin.joo
        """
        if len(data) < 1:
            self.logger.info("전송할 Data가 없습니다.")
            return

        # Data 형태에 따라 Json 형식으로 치환 할것
        # 고려할 문제이나 없는게 좋을 듯. ( 문자열만 가능한 상태 )

        # Data 전송
        try:
            # self.producer.poll(0)
            if keyvalue == '':
                self.producer.produce(topic, value=data.encode('utf-8'))  # , callback=self.delivery_report )
            else:
                self.producer.produce(topic, value=data.encode('utf-8'),
                                      key=keyvalue)  # , callback=self.delivery_report )

            self.logger.info(" Push Complete!!! ")
        except Exception as e:
            self.logger.info(f"[Push Error] {e}")

    def flush(self):
        """
        db commit 과 같이, kafka 서버로 해당 이벤트 송신을 수행하는 메서드
        Author: jaemin.joo
        """
        self.producer.flush()


