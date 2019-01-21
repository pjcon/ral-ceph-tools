#!/bin/python
import rados,sys

def functional_test(data="test",conf="/etc/ceph/ceph.conf",key="/etc/ceph/ceph.client.admin.keyring",poolName="data-test",objectName="rados_functional_test/test"):

	cluster = rados.Rados(conffile=conf)
	#cluster = rados.Rados(conffile, conf = dict (keyring=keyringfile))
	
	cluster.connect()
	
	#open file like object, write to, read from, delete
	
	pools = cluster.list_pools()
	if poolName not in pools:
		print("bad pool name")
		sys.exit(0)
	
	#open
	#write
	#read
	#remove
	#close
	
	ioctx = cluster.open_ioctx(poolName)
	ioctx.write_full(objectName,data)
	print(ioctx.read(objectName))
	ioctx.remove_object(objectName)
	ioctx.close()
	
functional_test()
