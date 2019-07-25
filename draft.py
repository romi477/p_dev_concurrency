import gzip


with gzip.open('20170929000100.tsv.gz', 'rt') as readfile:
    with open('sample50.tsv', 'w+') as writefile:
        for i in range(50000):
            line = readfile.readline()
            writefile.write(line)

