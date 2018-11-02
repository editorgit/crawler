
# TLDS = ['lt', 'lv', 'ee', 'fi']
TLDS = ['ee']
# TLDS = ['fi'] - NOT ok
# TLDS = ['lt'] - ok
# TLDS = ['lv'] - ok
# TLDS = ['lt']

MAX_THREADS = 100

AIO_DNS_ERROR = 599

LOW_LIMIT = 500

MAX_DOMAINS = 3000
MAX_PAGES = 1500

FILETYPES = {'.jpg', '.png', '.jpeg', '.xml', '.pdf', '.mp4', '.mp3',
             '.gif', '.swf', '.css', '.js', '.scss', '.m4v', '.doc', '.docx', '.odt'}


STOP_WORDS = ['void(', '.jpg?', '.png?', 'css?', 'sid=', '&ver', 'jsessionid=', 'token=', 'skype:', 'javascript:', '(', ')']