# Kafka 연동을 위한 Python module import.
# pip install confluent_kafka 명령을 통해 해당 모듈을 import 해야 함
from os import name
from queue import Empty
from confluent_kafka import Consumer, KafkaError, TopicPartition
import time
import sys, pathlib
print(str(pathlib.Path().absolute()))
sys.path.append(str(pathlib.Path().absolute()))

from app.config import LoggerConfig
from app.config import ConfigParserConfig
from app.util.timer.RepeatedTimer import RepeatedTimer

logger = LoggerConfig.get_logger()
config_mapper = ConfigParserConfig.get_configparser()
'''
# topic
topic_title = ['current_price']

# kafka data 를 수신하기 위한 settings
# group.id => Consumer Group-id : 동일 그룹에선 offset 값 공유, 여러 Consumer가 읽더라도 중복 발생하지 않음.
# enable.auto.commit = false 인 경우 offset 저장되지 않음
settings = {
    'bootstrap.servers': config_mapper['KAFKA_SERVER']['URL'],
    'group.id': config_mapper['KAFKA_SERVER']['CONSUMER_GROUPID'], # Consumer(Client)당 grouid를 config.ini에 다르게 설정필요
    'enable.auto.commit': "false", # Consumer가 종료되었다면, 어디까지 읽었는지 offset위치를 저장해줄 것인가? true : false
    'session.timeout.ms': 10000,
    'auto.offset.reset': 'earliest'
}

# Kafka 에서 데이터를 읽어오는 object
# subscribe topic 값 변경 시 재생성 해야 하는지 확인 할
consumer = Consumer(settings)
logger.info(f"consumer가 할당받은 topic+partitions list: {consumer.list_topics('1').topics}")

# 해당 topic 에 대해 subscribe(listen)
# ex)c.subscribe( ['KR*'] ) or topic_name*
consumer.subscribe(topic_title)

logger.info("kafka consumer start!")

try:
    while True:
        msg = consumer.poll(0.1)

        # 마지막 데이터를 읽어온 경우, None 을 return. 따라서 break
        if msg is None:
            continue
            # break
        elif not msg.error():
            logger.info(f"offset: {msg.offset()}, Received message: {msg.value().decode('utf-8')}")
            # 이 부분에 DB INSERT 등의 처리가 필요 #

        elif msg.error().code() == KafkaError._PARTITION_EOF:
            logger.info(f'End of partition reached {msg.topic()}/{msg.partition()}')
        else:
            logger.info(f'Error occured: {msg.error().str()}')

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
'''

'''
하나의 client가 하나의 KafkaConsumer 인스턴스를 가질 수 있도록 하는 함수
Author: jaemin.joo
'''
def singleton(class_):
    instances = {}
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance

'''
Kafka Consumer로 topic을 등록하고, partition의 이벤트 데이터를 subscribe, unsubscribe할 수 있도록 하는 클래스
Author: jaemin.joo
'''
@singleton
class KafkaConsumer():

    def __init__(self):
        self.topic_names = []
        self.consumer = self.__init_consumer()
        self.msg_timer = None
    
   
    def __init_consumer(self):
        '''
        KafkaConsumer 인스턴스로 생성할 때, Consumer Config가 진행된 self.consumer를 활용할 수 있도록 init하는 메서드
        Author: jaemin.joo
        '''
        setting = {
            'bootstrap.servers': config_mapper['KAFKA_SERVER']['URL'],
            'group.id': config_mapper['KAFKA_SERVER']['CONSUMER_GROUPID'], # Consumer(Client)당 grouid를 config.ini에 다르게 설정필요
            'enable.auto.commit': "false", # Consumer가 종료되었다, 어디까지 읽었는지 offset위치를 저장해줄 것인가? true : false
            'session.timeout.ms': 10000,
            'auto.offset.reset': 'earliest'
        }
        return Consumer(setting)
    
    def setTopics(self, topic_names:list) -> None:
        '''
        Kafka consumer의 subscribe할 topic 리스트를 설정하는 메서드
        Author: jaemin.joo
        '''
        if topic_names == []:
            raise ValueError("topic의 개수가 1개 이상이어야 합니다")
        
        self.topic_names = topic_names
    
    
    def subscribe_price(self):
        '''
        Kafka Server에 consumer가 subscribe(topics)하여 이벤트로 등록된 실시세 정보를 받아오는 메서드
        - 메시지를 모두 받아도 이벤트를 unscribe할 때까지 루프가 돌아야 한다(고민해봐야 할 로직)
        - while 루핑(전역변수 flag이용?) 
        Author: jaemin.joo
        '''
        self.consumer.subscribe(self.topic_names)
        print(self.consumer.committed([TopicPartition("A000020",1,26)]))
        
        # Kafka Consumer는 싱글턴 패턴을 적용해야, 중복생성을 막을 수 있다고 판단
        try:
            self.msg_timer = RepeatedTimer(1, self.consumer.poll, 0.1)
            # msg = self.msg_timer.join()
            # print("실시간 시세 정보: ",msg)
        except KafkaError as e:
            logger.error(f"kafka consumer error: {e}")
        except Exception as e:
            logger.error(f"Exception: {e}")

    def unsubscribe_price(self):
        '''
        진행중인 timer를 종료시키고, consumer를 unsubscribe하는 기능
        Author: jaemin.joo
        '''
        if self.__is_created_timer():
            self.msg_timer.stop()
        self.consumer.unsubscribe()

    def __del__(self):
        self.__close_consumer()
        if self.__is_created_timer():
            self.msg_timer.stop()

    def __close_consumer(self):
        if self.consumer is not None:
            self.consumer.close()

    def __is_created_timer(self):
        if self.msg_timer == None:
            return False
        return True
        
        
consumer_test = KafkaConsumer()
consumer_test.setTopics(["A000020","A000060"])

consumer_test.subscribe_price()

time.sleep(10)
consumer_test.unsubscribe_price()
