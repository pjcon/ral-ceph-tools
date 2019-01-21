#!/bin/python
import rados
import time,sys,filecmp,os,copy
import argparse,subprocess
from random import randrange

from ConfigParser import ConfigParser as CP

DOC="""
A user script to run a basic analysis of echo through the three available 
transfer protocols. Each will be run after another to return a metrics
object that contains write time, read time, object size, and success statuses.

usage: echo-functional-test.py [rxg] [--rados] [--xrootd] [--gridftp]

    -r, --rados run a rados functional test
    -x, --xrootd    run an xrootd functional test
    -g, --gridftp   run a gridftp functional test

"""

#output help string whenever error (including no arguments given)
class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)

#NOTE errors when sizeBytes >= 100M, hard limit on object size, use rados striper to increase the size
    #must use subprocess rados --striper ...

#TODO testing S3
#TODO checksum files
#TODO include cluster name in metrics


#metrics that will be printed to file or stdout
metrics={
    'protocol':None,
    'conducted':None,
    'conducted_epoch':None,
    'time_write':None,
    'time_read':None,
    'object_size':None,
    'pool_name':None,
    'open_success':None,
    'stat_success':None,
    'read_success':None,
    'write_success':None,
    'copy_success':None,
    'remove_success':None}

config_file="echo-functional-test.conf"

def xrootd_functional_test(conf="",key="",poolName="",objectName="",localName="",keepFiles=False,objectSize=1024):
    if "" in [conf,key,poolName,objectName,localName]:
        print('xrootd_functional_test: argument missing')
        #return None

    timeEpoch=time.time()
    timeConducted=time.asctime(time.gmtime(timeEpoch))

    xrootdMetrics=copy.deepcopy(metrics)
    xrootdMetrics['protocol'] = 'xrootd'
    xrootdMetrics["conducted"] = timeConducted
    xrootdMetrics["conducted_epoch"] = timeEpoch
    xrootdMetrics["pool_name"] = poolName

    print("XROOTD FUNCTIONAL TEST NOT IMPLEMENTED")
    return xrootdMetrics

def gridftp_functional_test(conf="",key="",poolName="",objectName="",localName="",keepFiles=False,objectSize=1024):
    if "" in [conf,key,poolName,objectName,localName]:
        print('gridftp_functional_test: argument missing')
        #return None

    timeEpoch=time.time()
    timeConducted=time.asctime(time.gmtime(timeEpoch))

    gridftpMetrics=copy.deepcopy(metrics)
    gridftpMetrics['protocol'] = 'gridftp'
    gridftpMetrics["conducted"] = timeConducted
    gridftpMetrics["conducted_epoch"] = timeEpoch
    gridftpMetrics["pool_name"] = poolName
    print("GRIDFTP FUNCTIONAL TEST NOT IMPLEMENTED")
    return gridftpMetrics

#ps1 = subprocess.Popen(cmd, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
#output, err = ps1.communicate()

def rados_functional_test(conf="",key="",poolName="",objectName="",localName="",keepFiles=False,objectSize=1024):
    #NOTE rados functional test does not issue a copy command because there does not appear to be one

    if "" in [conf,key,poolName,objectName,localName]:
        print('rados_functional_test: argument missing')
        return None

    radosMetrics=copy.deepcopy(metrics)
    radosMetrics['protocol'] = 'rados'

    timeEpoch=time.time()
    timeConducted=time.asctime(time.gmtime(timeEpoch))

    localNamePre=localName
    localNamePost=localName + ".post"

    if not os.path.exists(localNamePre):
        gen_data(name=localNamePre, sizeBytes=objectSize)
    else:
        print("Using existing file {} for test object".format(localNamePre))
        
    cluster = rados.Rados(conffile=conf)
    #cluster = rados.Rados(conffile, conf = dict (keyring=keyringfile))
    
    cluster.connect()
    
    if not cluster.pool_exists(poolName):
        print("bad pool name")
        sys.exit(0)

    try:
        #open file like object, write to, read from, delete
        ioctx = cluster.open_ioctx(poolName)
        radosMetrics['open_success'] = True
    except:
        radosMetrics['open_success'] = False


    #time write action
    tic,toc,tac,toe=[],[],[],[]
    with open(localNamePre,'rb') as localDataFile:
        localData = localDataFile.read()

        try:
            tic.append(time.time())
            ioctx.write_full(objectName,localData)
            radosMetrics['write_success'] = True
        except:
            radosMetrics['write_success'] = False

        toc.append(time.time())

        """ while True:
            chunk=data.read(chunkSize)
            if not chunk:
                print("break")
                break
            print("chunk")
            ioctx.aio_append(objectName,chunk)"""

    #check that can read the object
    try:
        object_size,objectTime = ioctx.stat(objectName)
        radosMetrics["stat_success"] = True
    except:
        radosMetrics["stat_success"] = False

    if os.path.exists(localNamePost):
        os.remove(localNamePost)

    with open(localNamePost,'wb') as localDataFile:

        try:
            tac.append(time.time())
            localData=ioctx.read(objectName,length=object_size)
            radosMetrics["read_success"] = True
        except:
            radosMetrics["read_success"] = False
        toe.append(time.time())

        localDataFile.write(localData)


    if filecmp.cmp(localNamePre,localNamePost):
        print("RADOS SUCCESS: read and write file matching")
    else:
        print("RADOS FAIL: read and write file not matching")
    
    try:
        ioctx.remove_object(objectName)
        radosMetrics["remove_success"] = True
    except:
        radosMetrics["remove_success"] = False
    ioctx.close()
    
    #delete local files
    if not keepFiles:
        os.remove(localNamePre)
        os.remove(localNamePost)

    # assign metrics
    radosMetrics["conducted"] = timeConducted
    radosMetrics["conducted_epoch"] = timeEpoch
    radosMetrics["time_write"] = toc[0] - tic[0]
    radosMetrics["time_read"] = toe[0] - tac[0] 
    radosMetrics["object_size"] = object_size
    radosMetrics["pool_name"] = poolName


    return radosMetrics

def gen_data(sizeBytes,name="/tmp/gen_data-rand"):
    M=2**20
    sizeBytes=1024*sizeBytes
    sizeBytesStr=float(sizeBytes/M)
    sizeBytesSuff='M'
    if sizeBytesStr < 0.1:
        sizeBytesStr=sizeBytes/1024 
        sizeBytesSuff='k'

    print("Generating {}{} data {}".format(sizeBytesStr,sizeBytesSuff,name))
    try:
        with open("/dev/urandom",'rb') as r:
            dat=r.read(sizeBytes)
    except:
        print("Failed to read random data from /dev/urandom")
        return 1
        
    
    try:
        with open(name,'wb') as t:
            t.write(dat)
    except:
        print("Failed to write data to file {}".format(name))
        return 2

    return 0

def handler(config):
    parser = ArgumentParser(description="""Echo functional test for Rados, GridFTP, and XRootD pathway metrics.
This sript is suitable to be run by a user for debugging purposes.""")
    parser.add_argument('-a','--all', help='test all (equivalent to -rxg, or --rados --xrootd --gridftp)', action='store_true')
    parser.add_argument('-r','--rados', help='test Rados protocol for write, stat, read, delete functioning', action='store_true')
    parser.add_argument('-x','--xrootd', help='test XRootD protocol for write, copy, stat, read, delete functioning \033[31m(NOT IMPLEMENTED)\033[39m', action='store_true')
    parser.add_argument('-g','--gridftp', help='test GridFTP protocol for write, stat, read, delete functioning \033[31m(NOT IMPLEMENTED)\033[39m', action='store_true')
    parser.add_argument('-k','--keepfiles', help='retain test files rather than deleting', action='store_true')
    parser.add_argument('-o','--outname', help='ouptut metrics to file \033[92mOUTNAME\033[39m instead of standard output')
    parser.add_argument('-f','--outform', help='select output file format [\'txt\', \'json\'] available')
    #TODO n repeated tests with average and std, include meantime, stdtime, numreps metric

    #TODO currently setting shared across all three protocols
    parser.add_argument('-S','--objectsize', help='size of object in kb', type=int)
    parser.add_argument('-N','--objectname', help='name of object when on cluster', type=int)
    parser.add_argument('-P','--pool', help='name of pool')
    parser.add_argument('-L','--localname', help='name of local object file')

    #TODO check how xrd and gftp validate requests
    parser.add_argument('-C','--config', help='name of functional test configs file (defaults to {})'.format(config_file))
    parser.add_argument('-K','--keyfile', help='name of keyfile to validate access')

    if len(sys.argv) == 1:
        parser.print_help()
        parser.exit()
        
    
    args = parser.parse_args()

    if args.config:
        config.read(args.config)
    else:
        config.read(config_file)

    output={
        "rados":{
            "do":None,
            "confs":{}
            },
        "xrootd":{
            "do":None,
            "confs":{}
            },
        "gridftp":{
            "do":None,
            "confs":{}
            },
        "output":False,
        "outName":None,
        "outForm":None,
        "outForms":None,
    }

    def getConfs(proto=""):
        #DOC get configs or defaults from file
        confs = {}

        protos=['rados','gridftp','xrootd']

        if proto not in protos:
            print('getConfs error: argument must be one of %s' % protos)
            return confs

        if args.objectsize:
             confs['siz'] = int(args.objectsize)
        else:
            try:
                confs['siz'] = int(config.get(proto,'objectSize'))
            except:
                confs['siz'] = int(config.get('defaults','objectSize'))

        #get rados config filename
        if args.config:
            confs['conf'] = args.config
        else:
            confs['conf'] = config.get(proto,'conf')
            if not confs['conf']:
                confs['conf'] = config.get('defaults','conf')

        if args.pool:
            confs['pool'] = args.pool
        else:
            confs['pool'] = config.get(proto,'pool')
            if not confs['pool']:
                confs['pool'] = config.get('defaults','pool')
            
        if args.keyfile:
            confs['key'] = args.key
        else:
            confs['key'] = config.get(proto,'key')
            if not confs['key']:
                confs['key'] = config.get('defaults','key')

        if args.objectname:
            confs['obj'] = args.objectname
        else:
            confs['obj'] = config.get(proto,'objectName')
            if not confs['obj']:
                confs['obj'] = config.get('defaults','objectName')

        if args.localname:
            confs['loc'] = args.localname
        else:
            confs['loc'] = config.get(proto,'localName')
            if not confs['loc']:
                confs['loc'] = config.get('defaults','localName')

        return confs

    doRados=False
    doXrootd=False
    doGridftp=False
    if args.all:
        print("=== Pre-test all protocols")
        doRados=True
        doXrootd=True
        doGridftp=True
        output["rados"]["confs"] = getConfs('rados')
        output["rados"]["do"] = True

        output["xrootd"]["confs"] = getConfs('xrootd')
        output["xrootd"]["do"] = True

        output["gridftp"]["confs"] = getConfs('gridftp')
        output["gridftp"]["do"] = True


    if args.rados:
        print("=== Pre-test rados protocol")
        doRados=True

        print("Getting rados configs")
        output["rados"]["confs"] = getConfs('rados')
        output["rados"]["do"] = True


    if args.xrootd:
        print("=== Pre-test xrootd protocol")
        doXrootd=True
        
        print("Getting gridftp configs")
        output["xrootd"]["confs"] = getConfs('xrootd')
        output["xrootd"]["do"] = True


    if args.gridftp:
        print("=== Pre-test gridftp protocol")
        doGridftp=True

        print("Getting xrootd configs")
        output["gridftp"]["confs"] = getConfs('gridftp')
        output["gridftp"]["do"] = True



    outForms = ['json','txt'] # first item is default
    if args.outname:
        outName=args.outname

        of=args.outform
        if of:
            if of in outForms:
                outForm = of
            else:
                print('Invalid output format, defaulting to {}'.format(outForms[0]))
                outForm = outForms[0] # default to first item
        else:
            ext=outName.split('.')[-1] 
            if ext in outForms:
                outForm = ext
            else:
                print('Cannot determine output format, defaulting to {}'.format(outForms[0]))
                outForm = outForms[0]


        output['output'] = True
        output['outForm'] = outForm
        output['outForms'] = outForms
        output['outName'] = outName

    return output

def printMetrics(allMetrics,fout=False,fobj=sys.stdout,raw=False):

    if raw:#simply write dict directly to file
        metricsFile.write(str(allMetrics))
        return

    for protoMetricKey in allMetrics.keys():
        metrics = allMetrics[protoMetricKey]
        if metrics is not None:
        
            #for each element in orders, init empty array under that key
            orders=['protocol','conducted','time','size','success'] #define order of metrics
            groupedKeys={}
            for order in orders: groupedKeys[order] = []
            groupedKeys['unordered'] = []
        
            used=[]#to help sort keys matched and not matched by orders array
            for metricKey in metrics.keys():
                for order in orders:
                    if order in metricKey:
                        groupedKeys[order].append(metricKey)
                        used.append(metricKey)
                
            for metricKey in metrics.keys():
                if metricKey not in used:
                    groupedKeys['unordered'].append(metricKey)
        
            for order in orders + ['unordered']:
                group=groupedKeys[order]
                for key in group:
                    metricString='{}\t:{}'.format(key,str(metrics[key]))
                    if fout:
                        fobj.write(metricString+'\n')
                    else:
                        print(metricString)
            
            if fout:
                fobj.write('\n')
        
    return


if __name__ == "__main__":
    config = CP()
    options=handler(config)
    
    allMetrics = {'rados':None,'xrootd':None,'gridftp':None}
    radosMetrics = {}
    xrootdMetrics = {}
    gridftpMetrics = {}

    if options["rados"]["do"]:
        print("=== Performing rados functional test")
        radosMetrics = rados_functional_test( \
            conf=options["rados"]["confs"]["conf"], \
            key=options["rados"]["confs"]["key"], \
            poolName=options["rados"]["confs"]["pool"], \
            objectName=options["rados"]["confs"]["obj"], \
            localName=options["rados"]["confs"]["loc"], \
            objectSize=options["rados"]["confs"]["siz"],\
            )
        allMetrics['rados'] = radosMetrics

    if options["xrootd"]["do"]:
        print("=== Performing xrootd functional test")
        xrootdMetrics = xrootd_functional_test( \
            conf=options["xrootd"]["confs"]["conf"], \
            key=options["xrootd"]["confs"]["key"], \
            poolName=options["xrootd"]["confs"]["pool"], \
            objectName=options["xrootd"]["confs"]["obj"], \
            localName=options["xrootd"]["confs"]["loc"], \
            objectSize=options["xrootd"]["confs"]["siz"],\
        )
        allMetrics['xrootd'] = xrootdMetrics

    if options["gridftp"]["do"]:
        print("=== Performing gridftp functional test")
        gridftpMetrics = gridftp_functional_test( \
            conf=options["gridftp"]["confs"]["conf"], \
            key=options["gridftp"]["confs"]["key"], \
            poolName=options["gridftp"]["confs"]["pool"], \
            objectName=options["gridftp"]["confs"]["obj"], \
            localName=options["gridftp"]["confs"]["loc"], \
            objectSize=options["gridftp"]["confs"]["siz"],\
        )
        allMetrics['gridftp'] = gridftpMetrics

    if options['output']:
        metricsFilename = options['outName']
        overwrite=True
        fileOutput=True
        wra='w'
        if os.path.exists(metricsFilename):
            overwrite=''
            while True:
                overwrite = raw_input('Metrics file already exists. overwrite/[append]? y/N/[a]:')
                if overwrite.lower() in ['a','append']:
                    wra='a'
                    break
                elif overwrite.lower() in ['y','yes']:
                    wra='w'
                    break
                elif overwrite.lower() not in ['a','append','y','yes']:
                    #xxx=randrange(100,999)
                    xxx=time.strftime('%H-%M-%S-%a-%d-%b-%Y')
                    metricsFilename = metricsFilename + '.' + str(xxx) + '.new'
                    print("Will write to {}".format(metricsFilename))
                    wra='a'
                    break
                print("bad response")

        #open file and decide format to write
        with open(metricsFilename,wra) as metricsFile:
            print("Writing to {}".format(metricsFilename))
            if options['outForm'] == 'txt':
                printMetrics(allMetrics=allMetrics,fout=fileOutput,fobj=metricsFile,raw=False)
            elif options['outForm'] == 'json':
                printMetrics(allMetrics=allMetrics,fout=fileOutput,fobj=metricsFile,raw=True)

    else:
        

        if options["rados"]["do"]:
            print("\n=== RADOS ===")
            printMetrics(allMetrics)
            print("=== END RADOS ===\n")
    
        if options["xrootd"]["do"]:
            print("=== XROOTD ===")
            printMetrics(allMetrics)
            print("=== END XROOTD ===\n")
    
        if options["gridftp"]["do"]:
            print("=== GRIDFTP ===")
            printMetrics(allMetrics)
            print("=== END GRIDFTP ===\n")

    #TODO exit status using sys.exit()
    return 0



