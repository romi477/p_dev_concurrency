with open('20170929000100.tsv', 'r') as readfile:
    with open('sample.tsv', 'w+') as writefile:
        for i in range(100000):
            line = readfile.readline()
            writefile.write(line)

