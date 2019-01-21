#!/usr/bin/python

import subprocess

ps = subprocess.Popen(('ls', '-l'), stdout=subprocess.PIPE)
output = subprocess.check_output(('grep', 'subprocess-pipe.py'), stdin=ps.stdout)
ps.wait()

print("success")
print(output)
