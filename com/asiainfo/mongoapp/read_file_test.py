import logging
from itertools import islice

logging.basicConfig(level=logging.INFO,
                    filename='/Users/mtr/PycharmProjects/mongoQuery/resource/log/emit_stat_load.log',
                    filemode='w',
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')

if __name__ == '__main__':
    N = 2
    X = 5
with open('/Users/mtr/PycharmProjects/mongoQuery/resource/exception/kafka.log') as f_input:
    for row in islice(f_input, N, None):
        logging.info(row.strip())
