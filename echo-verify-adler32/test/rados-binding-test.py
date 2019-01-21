#!/bin/python

#see the following address for rados python api docs
#http://docs.ceph.com/docs/giant/rados/api/python/#writing-and-reading-xattrs

import rados

conffile="/etc/ceph/ceph.conf"
cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')

keyringfile = "/etc/ceph/ceph.client.admin.keyring"
#cluster = rados.Rados(conffile, conf = dict (keyring=keyringfile))

#cluster.conf_get('mon initial members')

cluster.connect()
print ("\nCluster ID: " + cluster.get_fsid())

print ("\n\nCluster Statistics")
print ("==================")
cluster_stats = cluster.get_cluster_stats()

for key, value in cluster_stats.iteritems():
        print key, value

pools = cluster.list_pools()

#cannot create pools so easily
#cluster.create_pool("cjp-pybind-test")
#cluster.delete_pool("cjp-pybind-test")

#open file like object, write to, read from, delete
poolName="data-test"
objectName="pybind-test-write-hw"
testString="This is a test string."
ioctx = cluster.open_ioctx(poolName)

try:
	#write
	ioctx.write_full(objectName,testString)

	#read
	print(ioctx.read(objectName))

	#delete
	ioctx.remove_object(objectName)
except:
	print("something failed")

ioctx.close()

