#!/usr/bin/env python

import socket
import time
import signal
import sys
import operator
from datetime import datetime

def signal_handler(signal, frame):
    print('Program Interrupted. Bye!')
    sys.exit(0)

def monitor():
    signal.signal(signal.SIGINT, signal_handler)

    UDP_IP = socket.gethostbyname(socket.gethostname())
    UDP_PORT = 9999

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))

    nodes = {}

    while True:
        data, addr = sock.recvfrom(1024)
        ip = addr[0]
        # print "from ", ip, ": ", data
        nodes[ip] = time.time()
        print datetime.now()
        print '  ID         server IP                last report'
        print '-------------------------------------------------'

        i = 0
        do_sort = True
        if do_sort:
            sorted_list = sorted(nodes.items(), key=operator.itemgetter(1))
            for t in sorted_list:
                print '{:4d}{:>18s} {:>25s}'.format(i, t[0], datetime.fromtimestamp(t[1]).strftime("%Y-%m-%d %H:%M:%S"))
                i = i+1
        else:
            for n in nodes:
                print '{:4d}{:>18s} {:>25s}'.format(i, n, datetime.fromtimestamp(nodes[n]).strftime("%Y-%m-%d %H:%M:%S"))
                i = i+1

        print '-------------------------------------------------'
        print ''

def main():
    monitor()

if __name__ == "__main__":
    main()

