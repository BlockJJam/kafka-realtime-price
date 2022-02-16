import confluent_kafka
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
import time

from app.config import ConfigParserConfig
from app.config import LoggerConfig

'''
Kafka Server의 resource들을 핸들링하는 클래스
Author: jaemin.joo
'''
class KafkaAdmin():
    def __init__(self):
        self.config_mapper = ConfigParserConfig.get_configparser()
        self.logger = LoggerConfig.get_logger()
        self.admin = AdminClient( self.__setting_admin() )
        
    def __setting_admin(self):
        return { 
            'bootstrap.servers' : self.config_mapper['KAFKA_SERVER']['URL']
        }

    def create_topics(self, topic_names):
        '''
        
        '''
        new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1, config={'retention.ms':86400000}) for topic in topic_names]
        
        # topic을 만든 결과를 dict로 return하여 결과를 확인해주는 기능
        # python의 비동기 병렬처리 future를 이용한 듯 하다 reference에 <topic, future>를 return한다고 제공
        futures = self.admin.create_topics(new_topics)

        for topic, f in futures.items():
            try:
                f.result()
                self.logger.debug(f"Topic {topic} created.")
            except Exception as e:
                self.logger.error(f"Failed to create topic {topic}: {e}")

    def delete_topics(self, topic_names):
        futures = self.admin.delete_topics(topic_names)

        for topic, f in futures.items():
            try:
                f.result()
                self.logger.debug(f"Topic {topic} deleted.")
            except Exception as e:
                self.logger.error(f"Failed to delete topic {topic}: {e}")

    