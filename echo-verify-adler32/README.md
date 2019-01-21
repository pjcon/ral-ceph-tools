## Task
Write a script utility that compares the stored adler32 checksum with the actual checksum for a file on any pool given a set of command line arguments. This script cannot be reliant on potentially inaccessible elements like the 'xrd' command.

## Issues
There has been difficulty in the past to create this script owing to the binary format of the adler32 xattribute, along with existing manual methods which have reduced the incentive. Erasure code integrity checking (scrubbing) is able to check for changes in object data while the file objects are stored, but this does not guarantee that the file on echo is equivalent to the version kept by the service user.

A script has been written to check the adler32 checksum of a single large object file on echo, but does not use the striper option since the python library does not support it. The current process turns out to be slow to check large numbers of objects (~20s per GB) using the basic non-striper method, and cannot check files that are split into objects since it cannot piece them together. Striper draws objects from multiple disk drives, so theoretically it should obtain a large file faster in proportion to the number of disk drives providing the data.

Storage is a limiting factor on the machines performing the check which means that a rolling checksum on segments of the streamed file may be the preferred method. 

## Suggestions
- Check ral-ceph-tools github repo for the latest version of the current scripts
- Use the python file striper-adler32.py as a starting point, or a pointer.
- Use subprocess to implement the striper functionality (see commented code in get\_cluster\_object' function)
- Write a short script which accepts a file though stdout and outputs the adler32 checksum (rather than operating on a file on system). rados can output to stdout by adding a '-' in place of the output filename.
- It might be entirely reasonable to use subprocess commands in place of librados python commands
