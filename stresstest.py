import socket
import client
from random import randint
import threading
import time
host = socket.gethostname()

def createtask(ports,name):
    for i in range(10):
        r=randint(0, len(ports) - 1)
        c = client.client(host, ports[r])
        c.createoperation(name+"_"+str(i), "sample.txt")
        c.close()


def main():
    start_time = time.time()
    threads=[]
    # creating clients
    for i in range(10):
        t = threading.Thread(target=createtask,args=([53272,58431,61305],"client"+str(i)))
        t.daemon = True
        threads.append(t)

    for i in range(10):
        threads[i].start()

    for i in range(10):
        threads[i].join()

    print "time: "+str(time.time()- start_time)

main()