#!/usr/bin/env python

#TODO modify all references to nagios into user script analogs

import subprocess
import os
import os.path
import sys
import tempfile
import socket
import atexit
import hashlib

from datetime import datetime
from ConfigParser import ConfigParser as CP

NAG_OK       = 0
NAG_WARNING  = 1
NAG_CRITICAL = 2
NAG_UNKNOWN  = 3

#config_file='/usr/lib/nagios/plugins/ceph/xrootd_test.conf'
config_file='./xrootd_test.conf'
base_copy_cmd = ['xrdcp', '--silent', '--force']

def set_environment(conf):
    usercert = conf.get('exports','X509_USER_CERT')
    userkey = conf.get('exports','X509_USER_KEY')
    validity = conf.get('exports','valid')
    grid_proxy_cmd = ['grid-proxy-init', '-cert', usercert,
    '-key', userkey, '-valid', validity]
    subprocess.call(grid_proxy_cmd)
    if not os.path.exists('/tmp/x509up_u'+str(os.getuid())):
        #atexit.register(report, hostname, nagios_service_name, NAG_CRITICAL, "CRITICAL: Could not generate grid proxy", config)
        print('atexit.register(report, hostname, nagios_service_name, NAG_CRITICAL, "CRITICAL: Could not generate grid proxy", config)')
        print("critical: could not generate grid proxy")
    else:
        #atexit.register(cleanup_grid_proxy)
        #TODO
        print("clean up grid proxy")


#TODO not needed
"""def report(host, service_name, status, msg, nsca_conf):
    options = { k: v for k,v in nsca_conf.items('send_nsca') }
    cmd = ['/usr/sbin/send_nsca']
    try:
        cmd.append('-H')
        cmd.append(options['nagios_host'])
        cmd.append('-c')
        cmd.append(options['nsca_conf'])
    except KeyError as e:
        print str(e) + 'option should be defined'
        sys.exit(127)
    if 'port' in options:
        cmd.append('-p')
        cmd.append(options['port'])
    if 'timeout' in options:
        cmd.append('-to')
        cmd.append(options['timeout'])
    if 'delimiter' in options:
        cmd.append('-d')
        cmd.append(options['delimiter'])

    delim = options['delimiter'] if 'delimiter' in options else '\t'
    msg = delim.join([host, service_name, str(status), msg, '\n'])
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    p.communicate(input=msg)
    p.stdin.close()
    sys.exit(status)
"""

def cleanup_grid_proxy():
    gpc = '/tmp/x509up_u' + str( os.getuid() )
    if os.path.exists(gpc):
        os.remove(gpc)

def cleanup_local(fname):
    if os.path.isfile(fname):
        os.remove(fname)

def cleanup_remote(remote_path):
    subprocess.call(['xrdfs', socket.getfqdn(), 'rm', remote_path])

def gen_random(size, blocksize):
    with open('/dev/urandom', 'r') as rng, tempfile.NamedTemporaryFile(delete=False) as tf:
        while tf.tell() < (size):
            tf.write(rng.read(blocksize))
    #atexit.register(cleanup_local, tf.name)
    print('atexit.register(cleanup_local, tf.name)')
    return tf.name

def sha1_local_file(fname, blocksize):
    with open(fname, 'rb') as tmpf:
        tmpsha1 = hashlib.sha1()
        while True:
            tmpblk = tmpf.read(blocksize)
            if not tmpblk:
                break
            tmpsha1.update(tmpblk)
    return tmpsha1.hexdigest()

def gen_remote_url(**kwargs):
    return "root://{fqdn}/{pool}:{token}/{rfname}"\
    .format(fqdn=kwargs['fqdn'], pool=kwargs['pool'], token=kwargs['token'], rfname=kwargs['rfname'])

def gen_remote_obj_name(**kwargs):
    return "{pool}:{token}/{rfname}".format(pool=kwargs['pool'], token=kwargs['token'], rfname=kwargs['rfname'])

def write_to_remote(src, remote_filename):
    dst = gen_remote_url(fqdn=fqdn, pool=pool, token=token, rfname=remote_filename)
    cmd = base_copy_cmd + [src, dst]
    try:
        rc = subprocess.check_call(cmd)
        remote_obj_name = gen_remote_obj_name(pool=pool, token=token, rfname=remote_filename)
        #atexit.register(cleanup_remote, remote_obj_name)
        print('atexit.register(cleanup_remote, remote_obj_name)')
    except subprocess.CalledProcessError as e:
        fallback_read()


def read_from_remote(remote_fn, dst):
    src = gen_remote_url(fqdn=fqdn, pool=pool, token=token, rfname=remote_fn)
    cmd = base_copy_cmd + [src, dst]
    try:
        subprocess.check_call(cmd)
        #atexit.register(cleanup_local, dst)
        print('atexit.register(cleanup_local, dst)')
    except subprocess.CalledProcessError as e:
        #atexit.register(report, hostname, nagios_service_name, NAG_CRITICAL, "CRITICAL: Failed to read remote file", config)
        print('atexit.register(report, hostname, nagios_service_name, NAG_CRITICAL, "CRITICAL: Failed to read remote file", config)')
        sys.exit()

def fallback_read():
    try:
        read_from_remote(fallback_filename,fallback_filename+'-rtn')
        if fallback_only:
            #atexit.register(report, hostname, nagios_service_name, NAG_OK, "OK: XRootD check successful (fallback only mode)", config)
            print('atexit.register(report, hostname, nagios_service_name, NAG_OK, "OK: XRootD check successful (fallback only mode)", config)')
        else:
            #atexit.register(report, hostname, nagios_service_name, NAG_WARNING, "WARNING: Failed to write remote file, fallback read successful", config)
            print('atexit.register(report, hostname, nagios_service_name, NAG_WARNING, "WARNING: Failed to write remote file, fallback read successful", config)')
    except:
        pass
    sys.exit()

if __name__ == '__main__':

    config = CP()
    config.read(config_file)

    hostname = socket.gethostname().split('.')[0]
    nagios_service_name = 'cjp - Ceph XRootD functional test'

    tempfsize = config.getint('tempfile','size') # tmpfsize=67108864, 64MB
    tempfblksize = config.getint('tempfile','blocksize') # tmpfblksize=4194304, 4MB

    fallback_filename = config.get('fallback','persistent_file_name') #TODO remove fallback / find out how works
    fallback_only = config.getboolean('fallback','fallback_only')

    #call grid_proxy_cmd = ['grid-proxy-init', '-cert', usercert, '-key', userkey, '-valid', validity]
    #then apparently immediately delete the created tmpfile
    set_environment(config)

    fqdn = socket.getfqdn()
    pool = config.get('storage','pool') # pool=dteam
    token = config.get('storage','token') # token=test

    if not fallback_only:
        # Generate random file
        tempfname = gen_random(tempfsize, tempfblksize)
        # Compute SHA1 of random file
        tempfsha1 = sha1_local_file(tempfname, tempfblksize)

        #remote_filename = "nagios_check_xrootd_{date}_{gateway}"\
        remote_filename = "cjp_check_xrootd_{date}_{gateway}"\
        .format(date=datetime.now().strftime("%Y%m%d_%H%M%S"),
            gateway=hostname)
        # remote_url = "root://{fqdn}/{pool}:{token}/{rfname}"\
        # .format(fqdn=fqdn, pool=pool, token=token, rfname=remote_filename)

        # Copy file into Echo using GridFTP
        write_to_remote(tempfname, remote_filename)
        returnfname = tempfname+'-rtn'

        # Copy file out using XRootD
        read_from_remote(remote_filename, returnfname)
        # Compute SHA1 of returned file
        returnsha1 = sha1_local_file(returnfname, tempfblksize)
        # Compare SHA1s
        if returnsha1 == tempfsha1:
        # Report to Nagios
            #atexit.register(report, hostname, nagios_service_name, NAG_OK, "OK: XrootD check successful.", config)
            print('atexit.register(report, hostname, nagios_service_name, NAG_OK, "OK: XrootD check successful.", config)')
            sys.exit()
        else:
            #atexit.register(report, hostname, nagios_service_name, NAG_CRITICAL, "CRITICAL: Files did not match", config)
            print('atexit.register(report, hostname, nagios_service_name, NAG_CRITICAL, "CRITICAL: Files did not match", config)')
            sys.exit
    else:
        print("not using fallback option")
        print("exit")
        sys.exit()

        fallback_read()
        # should not normally reach this
        #atexit.register(report, hostname, nagios_service_name, NAG_UNKNOWN, "UNKNOWN: Check unknown state, fallback only mode", config)
        print('atexit.register(report, hostname, nagios_service_name, NAG_UNKNOWN, "UNKNOWN: Check unknown state, fallback only mode", config)')
        sys.exit()
