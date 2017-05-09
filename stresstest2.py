import socket
import client
from random import randint
import threading
import time
import balancer
#host = ["152.46.16.201","152.46.19.121","152.46.17.67","152.56.17.118"]

def createtask(hosts,ports,name):
    for i in range(1):
        r=randint(0, len(ports) - 1)
        c = client.client(hosts[0], ports[0])
        filename="duplicate"
        c.createoperation(name+"_N"+str(i),filename+".txt" )
        c.close()


def main():
    start_time = time.time()
    hosts, ports = balancer.selectAllClient()
    threads=[]
    # creating clients
    for i in range(10):
        t = threading.Thread(target=createtask,args=(hosts,ports,"client"+str(i)))
        t.daemon = True
        threads.append(t)

    for i in range(10):
        threads[i].start()

    for i in range(10):
        threads[i].join()

    print "time: "+str(time.time()- start_time)

main()