import os
import sys
import random


dirname = sys.argv[1]
filecount = int(sys.argv[2])
minsize = int(sys.argv[3])
maxsize = int(sys.argv[4])

if not os.path.exists(dirname):
    os.mkdir(dirname)

for _ in range(filecount):
    filename = os.urandom(8).encode('hex') + '.bin'
    path = os.path.join(dirname, filename)
    fd = open(path, 'wb')
    size = random.randint(minsize, maxsize) * (1024 * 1024)
    print 'Creating random data for file', path, size
    fd.write(os.urandom(size))
    fd.close()

