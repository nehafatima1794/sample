import os
import csv
import redis
from urllib2 import urlopen, URLError, HTTPError
from zipfile import ZipFile


def dlfile(url):
    try:
        f = urlopen(url)
        print "downloading" + url
        with open(os.path.basename(url), "wb") as local_file:
            local_file.write(f.read())
    except HTTPError, e:
        print "URL Error:", e.code, url
    except URLError, e:
        print "URL Error:", e.reason, url


def main():
    for index in range(150, 151):
        url = "https://www.bseindia.com/download/BhavCopy/Equity/EQ071218_CSV.ZIP"
        dlfile(url)
        file_name = "EQ071218_CSV.ZIP"
        with ZipFile(file_name, 'r') as get:
            get.printdir()
            print('Extracting all the file now')
            get.extractall()
            print('done!')
        with open('EQ071218.CSV', 'r') as csvFile:
            reader = csv.reader(csvFile)
            for row in reader:
                print(row)
            csvFile.close()


redisClient = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)
redisClient.hset("string", "code", "500002")
redisClient.hset("string", "name", "ABB LTD")
redisClient.hset("string", "open", "1375")
redisClient.hset("string", "high", "1382.85")
redisClient.hset("string", "low", "1365.45")
redisClient.hset("string", "close", "1374.3")
print("The keys and values present in redis are")
print(redisClient.hgetall("string"))
redisClient.hset("string1", "code", "500010")
redisClient.hset("string1", "name", "APPLE FINANCE")
redisClient.hset("string1", "open", "1.78")
redisClient.hset("string1", "high", "1.78")
redisClient.hset("string1", "low", "1.78")
redisClient.hset("string1", "close", "1.78")
print("The keys and values present in redis are")
print(redisClient.hgetall("string1"))
redisClient.hset("string2", "code", "500039")
redisClient.hset("string2", "name", "BANJO PROD")
redisClient.hset("string2", "open", "169")
redisClient.hset("string2", "high", "169")
redisClient.hset("string2", "low", "165.75")
redisClient.hset("string2", "close", "167.3")
print("The keys and values present in redis are")
print(redisClient.hgetall("string2"))
redisClient.hset("string3", "code", "500089")
redisClient.hset("string3", "name", "DIC INDIA")
redisClient.hset("string3", "open", "397")
redisClient.hset("string3", "high", "400")
redisClient.hset("string23", "low", "380")
redisClient.hset("string3", "close", "383.65")
print("The keys and values present in redis are")
print(redisClient.hgetall("string3"))


if __name__ == '__main__':
    main()