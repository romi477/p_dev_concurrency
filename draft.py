import gzip
from multiprocessing.dummy import Pool
from time import time
from functools import partial

def fun(a, b):
    print('a + b', a, b)
    return a + b

fun2 = partial(fun, b=1)



fun2(2)








# def func(num):
#     lst = []
#     for _ in range(num):
#         lst.append(num*2)
#
#
# args = [10000000, 10000000, 10000000]
#
# t1 = time()
#
# for i in args:
#     func(i)
# # pool = Pool(3)
# # result = pool.map(func, args)
# # pool.close()
# # pool.join()
#
# print('executing time: ', time() - t1)




# with gzip.open('big_tsv/20170929000100.tsv.gz', 'rt') as readfile:
#     with open('sample10.tsv', 'w+') as writefile:
#         for i in range(10000):
#             line = readfile.readline()
#             writefile.write(line)

