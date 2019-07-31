import multiprocessing as mp
import queue
from time import time


# with gzip.open('big_tsv/20170929000100.tsv.gz', 'rt') as readfile:
#     with open('sample10.tsv', 'w+') as writefile:
#         for i in range(10000):
#             line = readfile.readline()
#             writefile.write(line)


def do_job1(q):
    x = []
    while not q.empty():
        try:
            task = q.get(timeout=0.1)
        except queue.Empty:
            print('Queue empty exception')
            break
        # print('current process: ', mp.current_process().name)
        # print('TASK: ', task)
        x.append(task**2)


def main():
    que = mp.Manager().Queue()

    processes = []
    for i in range(1):
        alias = f'proc{i}'
        proc = mp.Process(target=do_job1, args=(que,), name=alias)
        processes.append(proc)

    for i in range(10000):
        que.put(i)

    for proc in processes:
        proc.start()

    for proc in processes:
        proc.join()
    print("Core: ", mp.cpu_count())

if __name__ == '__main__':
    t1 = time()
    main()
    print('TIME: ', time() - t1)
