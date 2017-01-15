import os
import collections
import hashlib
import logging
import functools
import argparse
import shelve

from multiprocessing.dummy import Pool
from queue import Queue, Empty
from contextlib import suppress


DATA_QUEUE_SIZE = 100
DATA_GETTING_TIMEOUT = 15
INITAL_DATA_READING_TIMEOUT = 1
DUMP_DB = 'duplicated.db'


logger = logging.getLogger('main')
sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)
sh.setFormatter(logging.Formatter(fmt='%(asctime)s %(thread)s %(message)s'))
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)


def get_file_data(data_queue, logger, absolute_file_name):
    with open(absolute_file_name, 'rb') as file:
        data = file.read()
        data_queue.put((absolute_file_name, data))
        logger.info('putted')


def main(dir, thread_count, dump):
    pool = Pool(processes=thread_count)
    all_files = []
    data_queue = Queue(DATA_QUEUE_SIZE)
    duplicated = collections.defaultdict(list)

    for dirpath, dirnames, filenames in os.walk(dir, onerror=lambda err: logger.error('walk error')):
        all_files.extend(os.path.join(dirpath, fn) for fn in filenames if not fn.lower().endswith('.mp4'))
    logger.info('All files count is %d' % len(all_files))

    pget_file_data = functools.partial(get_file_data, data_queue, logger)
    aresult = pool.map_async(pget_file_data, all_files)
    aresult.wait(INITAL_DATA_READING_TIMEOUT)  # to give the time to threads for the data_queue filling
    logger.info('waited and pooled all file names')

    with suppress(Empty):
        while not aresult.ready():
            afn, data = data_queue.get(timeout=DATA_GETTING_TIMEOUT)
            hash = hashlib.sha1(data).hexdigest()
            duplicated[hash].append(afn)
            logger.info('hashed')
            data_queue.task_done()

    duplicated = dict(filter(lambda item: len(item[1]) >= 2 , duplicated.items()))
    if not duplicated:
        logger.info('there are not duplicated photoes')
    elif dump:
        with shelve.open(DUMP_DB) as db:
            db['duplicated'] = duplicated
        logger.info('dumped to the %s' % DUMP_DB)
    else:
        logger.info(duplicated)

    logger.info('finished')


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('directory')
    arg_parser.add_argument('-tc', '--thread-count', type=int, default=os.cpu_count()*2)
    arg_parser.add_argument('-dump', action='store_true')
    args = arg_parser.parse_args()

    main(args.directory, args.thread_count, args.dump)
