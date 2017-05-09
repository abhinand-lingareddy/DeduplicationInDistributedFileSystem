from kazoo.client import KazooClient

from random import randint


def getzk():
    zk = KazooClient(hosts='152.46.16.201:2181')

    zk.start()

    return zk



def selectClient():
    zk=getzk()


    children=zk.get_children("/root")

    r=randint(0, len(children) - 1)

    info=zk.get("/root/"+children[r])[0]

    print "selected client "+str(info)

    host = info[:info.index(',')]
    port = int(info[info.index(',') + 1:])

    return host,port

def selectAllClient():
    zk = getzk()

    children=zk.get_children("/root")

    hosts=[]
    ports=[]

    for child in children:
        info=zk.get("/root/"+child)[0]
        host = hosts.append(info[:info.index(',')])
        port = ports.append(int(info[info.index(',') + 1:]))

    print " hosts,ports", hosts,ports



    return hosts,ports

#print selectAllClient()
