import os
TLDS = ['lt', 'lv', 'ee', 'fi', 'se', 'no', 'dk']
# TLDS = ['dk']
# TLDS = ['ee', 'fi']

DB_HOST = '95.217.8.25'
DB_USER = 'spidermen'
DB_PASS = 'aZ/dXtZ*Yf4P+UgJ'

MAX_THREADS = 100
# MAX_THREADS = 2

AIO_DNS_ERROR = 599

QUEUE_MAX_LIMIT = 10000
QUEUE_LOW_LIMIT = 500

MAX_DOMAINS = 3000
MAX_PAGES = 3000

FILE_TYPES = {'.jpg', '.png', '.jpeg', '.xml', '.pdf', '.mp4', '.mp3',
              '.gif', '.swf', '.css', '.js', '.scss', '.m4v', '.doc', '.docx', '.odt'}


STOP_WORDS = ['void(', '.jpg?', '.png?', 'css?', 'sid=', '&ver', 'jsessionid=', 'token=', 'skype:', 'javascript:',
              '(', ')']

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# LOGGER_LEVEL = 'DEBUG'
LOGGER_LEVEL = 'INFO'
# LOGGER_LEVEL = 'WARNING'

LOGGER_PATH = os.path.join(BASE_DIR, 'spider/logs/crawler/crawler.log')
LOGGER_FORMAT = '%(asctime)-15s %(message)s'
