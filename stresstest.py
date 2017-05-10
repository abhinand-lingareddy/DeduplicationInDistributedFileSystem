import socket
import client
from random import randint
import sys
import time
import balancer
#host = ["152.46.16.201","152.46.19.121","152.46.17.67","152.56.17.118"]
#host=socket.gethostbyname(socket.gethostname())
#host="152.46.16.201"
def createtask(hosts,ports,name):
    for i in range(1):
        r=randint(0, len(ports) - 1)
        c = client.client(hosts[r], ports[r])
        filename=sys.argv[2]
        c.createoperation(name+"A_"+str(i),filename )
        c.close()


def main():
    start_time = time.time()
    threads=[]
    # creating clients
    hosts,ports=balancer.selectAllClient()
    for i in range(20):
        createtask(hosts,ports,"c"+str(i))
    print "time: "+str(time.time()- start_time)

main()