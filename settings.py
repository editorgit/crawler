import os
TLDS = ['lt', 'lv', 'ee', 'fi', 'se', 'no', 'dk']
# TLDS = ['ee']
# TLDS = ['ee', 'fi']

DB_HOST = '95.217.8.25'
DB_USER = 'spidermen'
DB_PASS = 'aZ/dXtZ*Yf4P+UgJ'

MAX_THREADS = 100

AIO_DNS_ERROR = 599

LOW_LIMIT = 200

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

LOG_CRAWLER = os.path.join(BASE_DIR, 'spider/logs/crawler/crawler.log')
LOG_PARSER = os.path.join(BASE_DIR, 'spider/logs/crawler/parser.log')
FORMAT = '%(asctime)-15s %(message)s'
