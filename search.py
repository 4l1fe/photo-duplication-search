import os
import collections
import hashlib
import logging
import logging.config
import functools
import argparse
import shelve

from multiprocessing import Manager, Pool as ProcessPool
from multiprocessing.pool import ThreadPool
from multiprocessing.queues import Empty
from contextlib import suppress


LOGGER = 'main'
DATA_QUEUE_SIZE = 100
DATA_GETTING_TIMEOUT = 1
DUMP_DB = 'duplicated.db'


logger = logging.getLogger(LOGGER)


def put_file_data(data_queue, logger, absolute_file_name):
    with open(absolute_file_name, 'rb') as file:
        data = file.read()
        data_queue.put((absolute_file_name, data))
        logger.info('putted %d bytes' % len(data))


def full_duplicated(data_queue, duplicated):
    with suppress(Empty):
        while True:
            afn, data = data_queue.get(timeout=DATA_GETTING_TIMEOUT)
            hash = hashlib.sha1(data).hexdigest()
            print(afn, hash)
            if hash not in duplicated:
                duplicated[hash] = [afn]
            else:
                duplicated[hash].append(afn)
    print('worker exited')


def SearchManager(dir, thread_count, process_count, dump):
    logger.debug('thread count: %d' % thread_count)
    manager = Manager()
    data_queue = manager.Queue(DATA_QUEUE_SIZE)
    duplicated = manager.dict()
    all_files = []
    for dirpath, dirnames, filenames in os.walk(dir, onerror=lambda err: logger.error('walk error')):
        all_files.extend(os.path.join(dirpath, fn) for fn in filenames if not fn.lower().endswith('.mp4'))
    logger.info('All files count is %d' % len(all_files))

    def error_callback(aresult):
        raise aresult

    rpool = ThreadPool(thread_count)
    pget_file_data = functools.partial(put_file_data, data_queue, logger)
    rresult = rpool.map_async(pget_file_data, all_files,
                              error_callback=error_callback)
    logger.info('pooled all file names')


    hpool = ProcessPool(process_count)
    hresult = hpool.starmap_async(full_duplicated, [(data_queue, duplicated) for _ in range(process_count)],
                               error_callback=error_callback)
    rpool.close()
    rpool.join()
    hpool.close()
    hpool.join()


    duplicated = dict(filter(lambda item: len(item[1]) >= 2, duplicated.items()))
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
    arg_parser.add_argument('-tc', '--thread-count', type=int, default=os.cpu_count())
    arg_parser.add_argument('-pc', '--process-count', type=int, default=os.cpu_count())
    arg_parser.add_argument('-dump', action='store_true')
    args = arg_parser.parse_args()

    logging_conf = {
        'version': 1,
        'formatters': {
            'common': {'class': 'logging.Formatter',
                       'format': '%(asctime)s %(thread)s %(message)s'
            },
        },
        'handlers': {
            'stdout': {'class': 'logging.StreamHandler',
                       'level': 'DEBUG',
                       'formatter': 'common',
                       'stream': 'ext://sys.stdout'
           },
        },
        'loggers': {
            LOGGER: {'handlers': ['stdout'],
                     'level': 'DEBUG'
            },
        },
    }
    logging.config.dictConfig(logging_conf)

    SearchManager(args.directory, args.thread_count, args.process_count,  args.dump)
