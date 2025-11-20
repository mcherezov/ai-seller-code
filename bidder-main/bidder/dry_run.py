import logging
import argparse
import pickle
import lzma

from bidder import main


def dry_run():
    rootLogger = logging.getLogger()
    logFormatter = logging.Formatter('%(asctime)s\t%(filename)s:%(lineno)d\t%(levelname)s\t%(message)s')
    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(logFormatter)
    rootLogger.addHandler(stderr_handler)
    rootLogger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--fetched', help='get data from file')

    args = parser.parse_args()

    if args.fetched is not None:
        with lzma.open(args.fetched, 'rb') as fo:
            fetched = pickle.load(fo)
        main.set_bids(False, fetched, False)
    else:
        main.set_bids(False, None, True)


if __name__ == '__main__':
    dry_run()
