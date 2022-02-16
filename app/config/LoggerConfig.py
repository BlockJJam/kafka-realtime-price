import logging


def get_logger():
    '''
    logging을 통해, log 레벨에 따라 로그를 확인할 수 있도록 하고, 파일로 log를 저장하는 메서드
    Author: jaemin.joo
    '''
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(level=logging.DEBUG)
    
    file_handler = logging.FileHandler(f'C:/Users/TY/source/python/kafka_realtime_price/KafkaTest/logs/RealtimePriceProcess.log')
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    return logger

