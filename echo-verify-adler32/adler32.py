#!/usr/bin/env python3
import zlib
import sys
import os
import rados

import subprocess
import argparse

import atexit

import struct
import binascii

import time
"""DOC
try running on dev with following arguments
--objectName "/cjp/large1.adler32.test"
--pool "data-test"

Return codes
0 matching checksums
1 successful return with non matching checksums
2 execution halted early (subprocess errors, incorrect arguments)
3 unexpected error (error with code logic)
"""

### vim
#:set number relativenumber tabstop=4 expandtab
#:retab

"""
xrdcp -C | --cksum type[:value|print|source]
            obtains the checksum of type (i.e. adler32, crc32, or md5) from the source, computes the checksum at the destination, and verifies that they are the same. If a value is specified, it is used as
            the source checksum. When print is specified, the checksum at the destination is printed but is not verified.
"""

##necessary
#TODO choice to output log info to stdout, feed info to stderr
#TODO scan-all option, to scan every object within a given pool, need a clean method to exit
#TODO rados striper using subprocess

def cluster_init(conf="/etc/ceph/ceph.conf"):
    clust = rados.Rados(conffile=conf)
    #cluster = rados.Rados(conffile, conf = dict (keyring=keyringfile))
    
    try:
        clust.connect()
    except:
        print("Cannot open connection to cluster")
        return 1
    
    return clust

def cluster_xattr_exists(io,obj,xattr):
    #check all xattrs, if xattr we are checking exists, goodo
    xattrs = io.get_xattrs(obj)
    for xattrn, xattri in xattrs:
        if xattr == xattrn:
            return True

    print("xattr {} does not exist for this object".format(xattr))
    return False
    
def cluster_check_obj(io,obj):
    objs = io.list_objects()
    for obji in objs:
        if obj == obji:
            return True

    print("object {} does not exist in this pool".format(obj))
    return False
    
    """ while True :
            try :
                    rados_object = object_iterator.next()
                    print "Object contents = " + rados_object.read()
            except StopIteration :
                    break """

#def cluster_check_pool(clust,pool):
    #pools = clust.list_pools()
    #if pool not in pools:
        #print("pool {} does not exist in cluster".format(pool))
        #return False
    #return True

def cluster_open_pool(clust,poolName="data-test"):
    #if cluster_check_pool(clust,poolName):
    if clust.pool_exists(poolName):
        io = clust.open_ioctx(poolName)
        return io
    else:
        print("Cannot find pool")
        return 1

def cluster_object_exists(io,objectName):
    objectNames=io.list_objects()

    for objectNamei in objectNames:
        if objectName == objectNamei.key:
            return True

    return False

def cluster_get_xattr(io,objectName="/cjp/large1.adler32.test",checksumName="XrdCks.adler32"):
    if not cluster_object_exists(io,objectName):
        print("Object {} does not exist".format(objectName))
        return 2

    if cluster_xattr_exists(io,objectName,checksumName):
        checksum=io.get_xattr(objectName,checksumName)
    else:
        print("Object {} has no xattr named {}".format(objectName,checksumName))
        return 1

    return checksum


def cluster_get_object(io,objectName="/cjp/large1.adler32.test",fileName="/tmp/rados_functional_test_obj.delete"):
    
    cluster_object_exists(io,objectName)

    writeLog(logFile, objectName, label="OBJECT")

    if not cluster_object_exists:
        print("Object {} does not exist in pool".format(objectName))
        return 1
        

    size,mtime = io.stat(objectName)

    writeLog(logFile, str(size), label="SIZE")

    ##rados --striper get  /store/data/Run2018A/MET/RAW/v1/000/316/457/00000/0AF3F376-6859-E811-992F-FA163ED610A9.root -p cms /tmp/outfile
    #radosGetCmd=["rados"]
    #radosGetCmd.append("--striper") 
    #radosGetCmd.append("get")
    #radosGetCmd.append(objectName)
    #radosGetCmd.append("-p")
    #radosGetCmd.append(poolName)
    #radosGetCmd.append(fileName)
    #p,err = subprocess.call(radosGetCmd)

    buf=64
    off=0
    M=2**20
    #TODO locate list of 64M file chunks and loop through them, investigate parallel download similar to striper
    with open(fileName,'wb') as f:
        while True:
    
            try:
                chunk=io.read(objectName,buf *M,off *M)
                #print("write {}M".format(off))
                f.write(chunk)
            except:
                print("break")
                break

            off+=buf
            if off *M >= size: #NOTE hack ioctx does not return error on offset > size
                break

    #TODO write own striper, sum sequentially, delete per object, look at striper source code
    #TODO find object to test rados striper on
    #TODO /tmp is not a tmpfs, create own tmpfs


    if os.path.isfile(fileName):
        print("Finished downloading object: {}".format(fileName))

    print("Remote size: "+str(size))
    localSize=os.stat(fileName).st_size
    print("Local size: "+str(localSize))

    if size == localSize:
        return 0
    elif size == localSize:
        print("Size mismatch")
        return 1

    return 2
    
def writeLog(logFile,logString, label='', nodelim=False, nolabel=False):
    delim="\t"
    if nodelim:
        delim=""

    sep=":"
    if nolabel:
        sep=""

    with open(logFile,'a') as log:
        #logString="{}::{}::{}::stored:{}::calculated:{}\n".format(checkTime,success,objectName,storedAdler32,calculAdler32)
        log.write(delim+label+sep+logString)


def handler():
    #==========
    #parse args
    #==========
    parser = argparse.ArgumentParser(description="Compare stored adler32 checksum with the true checksum of the stored object")
    parser.add_argument('-p','--poolname', help='pool name',required=True)#mandatory
    parser.add_argument('-o','--objectname', help='name of object within pool')#mandatory
    parser.add_argument('-f','--localname', help='name of temporary filename to create')
    parser.add_argument('--keeplocal', help='keep downloaded copy of object',action='store_true')
    parser.add_argument('--all', help='iteratively step through all objects in pool',action='store_true')

    args = parser.parse_args()

    output={}
    
    if args.poolname:
        output['poolName']=args.poolname
    
    if args.keeplocal:
        output['keepLocal']=True
    else:
        output['keepLocal']=False


    output['objectNames']=None
    if args.objectname:
        output['objectName']=args.objectname
        output['allObjects']=False
    elif args.all:
        output['objectName']=None
        output['allObjects']=True
        

    if args.localname:
        output['localName']=args.localname
    else:
        output['localName']="/tmp/"+output['objectName'].replace('/','-')[1:] #remove leading -, prevent being read as command parameter
    

    #=========
    
    ##bypass parse args
    output['scriptName']=os.path.basename(__file__)
    output['checksumName']="XrdCks.adler32"
    output['logFile']="/tmp/{}.{}.log".format(output['scriptName'],output['checksumName'])


    return output


def main(options):
    """pseudo
    parse arguments
    set extra arguments, checksum type, logfile name, log string init
    connect to cluster
    set time in logs
    extract adler32 checksum
    download object and calculate adler32
    compare both checksums
    remove local object file
    """

    poolName=options['poolName']
    localName=options['localName']
    objectName=options['objectName']
    checksumName=options['checksumName']
    scriptName=options['scriptName']

    global logFile
    logFile = options['logFile']

    #or atexit.register(writeLog(logFile, '\n', nolabel=True, nodelim=True))
    writeLog(logFile, '\n', nolabel=True, nodelim=True)

    #keep time that process started
    checkTimeHuman=str(time.strftime('%H:%M:%S %a %d %b %Y'))
    checkTimeEpoch=str(time.time())
    writeLog(logFile, checkTimeHuman, nolabel=True, nodelim=True)
    writeLog(logFile, checkTimeEpoch, nolabel=True)
    
    #connect to the cluster, grab xattr data, download object
    cluster=cluster_init()
    if cluster == 1:
        print("Failure to connect to cluster")
        writeLog(logFile, "CONNECT-FAIL", label="EXIT")
        return 1
        
    ioctx=cluster_open_pool(cluster,poolName)
    if ioctx == 1:
        print("Failure to open pool")
        writeLog(logFile, "POOL-NONEXISTENT", label="EXIT")
        return 1

    if options['allObjects']:
        objectNames=ioctx.list_objects()
    
    checksumXattr=cluster_get_xattr(ioctx,objectName,checksumName)
    if checksumXattr == 1:
        writeLog(logFile, "MISSING-CHECKSUM", label="EXIT")
        return 1
    elif checksumXattr == 2:
        print("Object not found")
        writeLog(logFile, "NOT-FOUND", label="EXIT")
        return 1

    res=cluster_get_object(ioctx,objectName,localName) 
    if res == 1:
        print("Failure to retrieve object")
        writeLog(logFile, "NOT-RETRIEVED", label="EXIT")
        return 1
    if res == 2:
        print("Failure to correctly retrieve object")
        writeLog(logFile, "SIZE-MISMATCH", label="EXIT")
        return 1
    else: #if successfully downloaded
        if not options['keepLocal']:
            atexit.register(os.remove,options['localName'])
            atexit.register(print,'Removing '+options['localName'])

    ioctx.close()

    if options['keepLocal']:
        writeLog( logFile, localName, label="LOCAL")
    

    #==================================
    ##Extract checksum from xattr bytes
    #==================================
    
    #seek=32, leng=4 for raw bytes in the real data
    seek=12
    leng=4
    ends=seek+leng
    
    checksumLen = str(len(checksumXattr))#debug
    #print("checksum raw bytes len: ".format(str(checksumLen)))
    
    #-----------------------------
    ###Stored xattr checksum value
    storedAdler32 = binascii.b2a_hex(struct.unpack('={}s'.format(leng), checksumXattr[seek:ends])[0]).decode().upper()
    #-----------------------------
    #storedAdler32_2 = binascii.b2a_hex(struct.unpack('={}s'.format(leng), rawa32[seek:ends])[0])
    
    
    #=========================================
    ##Calculate checksum for downloaded object
    #=========================================
    
    #chunk large object file due to memory constraints
    def read_chunks(file_object, chunk_size=1024):
        while True:
                data = file_object.read(chunk_size)
                if not data:
                    break
                yield data
    
    #calculate running adler32 sum
    with open(localName,'rb') as f:
        zlibAdler32=zlib.adler32(f.read(2**20))
    
        for piece in read_chunks(f,2**20):
                zlibAdler32 = zlib.adler32(piece,zlibAdler32)
    
    #----------------------------
    ###True object checksum value
    calculAdler32=str(hex(zlibAdler32))[2:].upper()
    #----------------------------
    #[2:] cuts off the 0x from the string format of a hex number

    writeLog(logFile, checksumName, label="CHECKSUM")
    writeLog(logFile, storedAdler32, label='STORED-CHECKSUM')
    writeLog(logFile, calculAdler32, label='CACULATED-CHECKSUM')
    
    print("Stored     : {}".format(storedAdler32))
    print("Calculated : {}".format(calculAdler32))

    match = (calculAdler32 == storedAdler32)
    if match:
        print("Adler32 checksum: match")
        writeLog(logFile, 'MATCH', label="STATUS")
        return 0
    else:
        print("Adler32 checksum: no match")
        writeLog(logFile, 'FAIL', label="STATUS")
        return 2


    
if __name__ == "__main__":
    options=handler()
    #TODO put cluster open outside main
    #TODO collect success statuses for each in multi-write

    tic=time.time()
    ret=main(options)
    toc=time.time()

    print("Time taken : {:05.2f}s".format(toc-tic))
    writeLog(logFile,"{:05.2f}s".format(toc-tic), label="ELAPSED")

    sys.exit(ret)
