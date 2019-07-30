import os
import sys
import glob
import gzip
import logging
import memcache
import collections
from time import time
import queue
import appsinstalled_pb2
import multiprocessing as mp
import multiprocessing
import threading as th
from optparse import OptionParser
from functools import partial

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


class NoDaemonProcess(mp.Process):
    def _get_daemon(self):
        return False
    
    def _set_daemon(self):
        pass
    daemon = property(_get_daemon, _set_daemon)
    

class NoDaemonPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess
    

def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            memc = memcache.Client([memc_addr])
            memc.set(key, packed)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def insert_manager(in_queue):
    # processed = errors = 0
    while True:
        try:
            task = in_queue.get()
        except:
            break
        insert_appsinstalled(*task)
        # if ok:
        #     processed += 1
        # else:
        #     errors += 1
        # return processed, errors


def read_file(file, options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    processed = 1
    errors = 0
    logging.info('Processing %s' % file)
    in_queue = mp.Queue()

    sub_pool = mp.Pool(2)
    processing = sub_pool.map(insert_manager, [in_queue])

    with gzip.open(file, 'rt') as text:
        for line in text:
            line = line.strip()
            if not line:
                continue
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue
            memc_addr = device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                errors += 1
                logging.error("Unknow device type: %s" % appsinstalled.dev_type)
                continue
            in_queue.put((memc_addr, appsinstalled, options.dry))

    in_queue.join()

    err_rate = float(errors) / processed
    if err_rate < NORMAL_ERR_RATE:
        print(file, "Acceptable error rate (%s). Successfull load" % err_rate)
    else:
        print(file, "High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
    # dot_rename(fn)


def main(options):
    # pool = NoDaemonPool(processes=1)
    pool = mp.Pool(3)
    pool.map(partial(read_file, options=options), glob.iglob(options.pattern))
    pool.close()
    pool.join()


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        t1 = time()
        main(opts)
        print('executing time: ', time() - t1)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)