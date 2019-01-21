#!/usr/bin/env python

import subprocess
import os
import os.path
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


config_file='/usr/lib/nagios/plugins/ceph/gridftp_test.conf'
base_copy_cmd = ['globus-url-copy', '-q']


def set_environment(conf):
    exports = { k.upper(): v for k,v in conf.items('exports') }
    os.environ.update(exports)

def report(host, service_name, status, msg, nsca_conf):
    options = { k: v for k,v in nsca_conf.items('send_nsca') }
    cmd = ['/usr/sbin/send_nsca']
    try:
        cmd.append('-H')
        cmd.append(options['nagios_host'])
        cmd.append('-c')
        cmd.append(options['nsca_conf'])
    except KeyError as e:
        print str(e) + 'option should be defined'
        exit(255)
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
    exit(status)

def cleanup_local(fname):
    if os.path.isfile(fname):
        os.remove(fname)

def cleanup_remote(remote_url):
    cmd = ['gfal-rm', remote_url]
    subprocess.call(['gfal-rm', remote_url])

def gen_random(size, blocksize):
    with open('/dev/urandom', 'r') as rng, tempfile.NamedTemporaryFile(delete=False) as tf:
        while tf.tell() < (size):
            tf.write(rng.read(blocksize))
    atexit.register(cleanup_local, tf.name)
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

def write_to_remote(src, dst):
    cmd = base_copy_cmd + [src, dst]
    try:
        subprocess.check_call(cmd)
        atexit.register(cleanup_remote, dst)
    except subprocess.CalledProcessError as e:
        atexit.register(report, hostname, nagios_service_name, NAG_CRITICAL, "CRITICAL: Failed to write remote file", config)
        exit(e.returncode)

def read_from_remote(src, dst):
    cmd = base_copy_cmd + [src, dst]
    try:
        subprocess.check_call(cmd)
        atexit.register(cleanup_local, dst.replace('file://',''))
    except subprocess.CalledProcessError as e:
        atexit.register(report, hostname, nagios_service_name, NAG_CRITICAL, "CRITICAL: Failed to read remote file", config)
        exit(e.returncode)

if __name__ == '__main__':

    config = CP()
    config.read(config_file)

    hostname = socket.gethostname().split('.')[0]
    nagios_service_name = 'Ceph GridFTP functional test'

    tempfsize = config.getint('tempfile','size')
    tempfblksize = config.getint('tempfile','blocksize')

    set_environment(config)

# Generate random file
    tempfname = gen_random(tempfsize, tempfblksize)
# Compute SHA1 of random file
    tempfsha1 = sha1_local_file(tempfname, tempfblksize)

    local_url = "file://{fname}".format(fname=tempfname)

    fqdn = socket.getfqdn()
    pool = config.get('storage','pool')
    token = config.get('storage','token')

    remote_filename = "nagios_check_{date}_{gateway}"\
    .format(date=datetime.now().strftime("%Y%m%d_%H%M%S"),
        gateway=hostname)

    remote_url = "gsiftp://{fqdn}/{pool}:{token}/{rfname}"\
    .format(fqdn=fqdn, pool=pool, token=token, rfname=remote_filename)

# Copy file into Echo using GridFTP
    write_to_remote(local_url, remote_url)

    returnfname = tempfname+'-rtn'
    local_return_url = "file://{fname}".format(fname=returnfname)
# Copy file out using GridFTP
    read_from_remote(remote_url, local_return_url)
# Compute SHA1 of returned file
    returnsha1 = sha1_local_file(returnfname, tempfblksize)
# Compare SHA1s
    if returnsha1 == tempfsha1:
# Report to Nagios
        atexit.register(report, hostname, nagios_service_name, NAG_OK, "OK: GridFTP check successful.", config)
        exit(0)
    else:
        atexit.register(report, hostname, nagios_service_name, NAG_CRITICAL, "CRITICAL: Files did not match", config)
        exit(2)
