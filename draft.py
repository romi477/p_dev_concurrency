import gzip
import multiprocessing as mp
from multiprocessing.dummy import Pool
from time import time
from functools import partial
import queue


# with gzip.open('big_tsv/20170929000100.tsv.gz', 'rt') as readfile:
#     with open('sample10.tsv', 'w+') as writefile:
#         for i in range(10000):
#             line = readfile.readline()
#             writefile.write(line)


def do_job1(q):
    while not q.empty():
    # while True:
        try:
            task = q.get(timeout=0.1)
        except queue.Empty:
            print('Queue empty exception')
            break
        print('current process: ', mp.current_process().name)
        print('TASK: ', task)
        
        
def do_job2(q):
    while not q.empty():
    # while True:
        try:
            task = q.get(timeout=0.1)
        except queue.Empty:
            print('Queue empty exception')
            break
        print('current process: ', mp.current_process().name)
        print('TASK: ', f'{task}+')


def main():
    que = mp.Queue()
    
    # pool = mp.Pool(1)
    # res = pool.map(do_job, [que])
    # print('RES', res)
    proc1 = mp.Process(target=do_job1, args=(que,), name='proc1')
    proc2 = mp.Process(target=do_job2, args=(que,), name='proc2')
    
    proc1.start()
    proc2.start()
    
    for i in range(20):
        que.put(i)
        
    proc1.join()
    proc2.join()
    
if __name__ == '__main__':
    main()