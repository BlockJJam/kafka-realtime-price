import configparser
import os

path_current_dir = os.path.dirname(__file__)
path_config_dir = os.path.join(path_current_dir, 'config.ini') 


def get_configparser():
    '''
    private한 민감 정보를 "config.ini"에서 가지고 올 수 있도록 하는 함수
    Author: jaemin.joo
    '''
    config = configparser.ConfigParser()
    config.read(path_config_dir)
    return config