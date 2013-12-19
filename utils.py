#
# author: jinnan.qjn@alibaba-inc.com
#
# vimtab = 4 shiftstop = 4
#

import os
import sys
import commands
import subprocess
import socket
import signal
import re
import hashlib
import math

def check_string_match(string, pattern):
    rx = re.compile(str(pattern))
    rres = rx.search(string) 
    if rres is None:
        return -1;
    else: return rres 

def check_ip_addr(ip_addr):
    rx = re.compile("\d+.\d+.\d+.\d+")
    rres = rx.search(str(ip_addr))
    if rres is None:
        return -1
    else:
        nums = ip_addr.split(".")
#DEBUG:
        #for n in nums:
        #    print >> sys.stdout, int(n)
        for num in nums:
            if int(num) >= 0 and int(num) <= 255: continue
            else: return -1
        return rres
                

def check_online(ip_addr):
    if -1 == check_ip_addr(ip_addr): 
        print >> sys.stderr, "ip address is not good"
    pack_num = 4
    check_cmd = "".join(["ping ", ip_addr, " -c ", str(pack_num)])
    output = commands.getoutput(check_cmd)
    res_out = check_string_match(output.splitlines()[-2], "\d+\% packet loss")
#DEBUG:
    #print >> sys.stdout, (res_out, output.splitlines()[-2]) 
    if -1 != res_out:
        c_res = output.splitlines()[-2][res_out.start() - 2:res_out.end()] 
        print >> sys.stdout, "[cluster result: ", ip_addr, c_res, "]" 
    else:
        print >> sys.stderr, "packet result is not match"

def check_port(host_info):
    host, port = host_info
    rres = check_ip_addr(host_info[0])
    if -1 != rres:
        host = host[rres.start():rres.end()]
        so = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try: so.connect((host, int(port)))
        except: 
            print >> sys.stderr, "[ERROR: ip address: ", host, " port number: ", str(port), "is not good]"
            return 
        print >> sys.stdout, "[INFO: ip address: ", host, ", port number: ", str(port)," is okay]" 
    else:
        print >> sys.stderr, "[ERROR: host ip addr is not good]"

def check_all_svrs_online(svrs):
    count = 0; page_num = int(math.ceil(len(svrs) / 2.0)) 
    ms_mat = [[] for x in range(0, int(len(svrs)) / page_num + 1)] 
    for i in range(0, int(len(svrs) / page_num + 1)):
        for j in range(i * page_num , (i + 1) * page_num):
            try:
                if svrs[j]: ms_mat[i].append(svrs[j])
            except IndexError as index_e: continue
                
#FIXME: parellel instead of serial
#   but what I performed is so ugly, so improve this
    pid = os.fork()
    if pid:
        for ms in ms_mat[0]:
            check_port(ms)
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    else:
        try:
            for ms in ms_mat[1]:
                check_port(ms)
        finally: exit(signal.SIGHUP)


if "__main__" == __name__:
    print >> sys.stderr, "this module could not be used as main"
