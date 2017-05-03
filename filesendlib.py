import sendlib
import os

"""
converts number to bytes
"""
def no_to_bytes(no):
    b=str(no)
    non_zeroscounter=len(b)
    zeros=[]
    for i in range(4-non_zeroscounter):
        zeros.append("0")
    return "".join(zeros)+b

"""
converts bytes to number
"""
def bytes_to_no(b):
    return int(b)
"""
returns the storage path prefix
"""
def storagepathprefix(storage_path):
    if storage_path is not None:
        return storage_path + '/'
    else:
        return ""


def getfilepointer(storage_path,filename):
    storage_path = storagepathprefix(storage_path)
    f = open(storage_path + filename, 'wb')
    return f

def recvfile(storage_path,filename,s):
    f=getfilepointer(storage_path,filename)
    recvfilewithpointer(f,s)

def recvfilewithpointer(f,s):
    while (True):
        print "Receiving..."
        l = s.recv(1024)
        if l[-1]=='~':
            f.write(l[:-1])
            break
        f.write(l)
    f.close()
    print "Done Receiving"



def send_fromactual(file_path, s):
    # length=os.stat(file_path).st_size
    # length_str = no_to_bytes(length)
    # s.send(length_str)

    f = open(file_path, 'rb')
    while (True):
        l = f.read(1024)
        if len(l)<1024:
            l=l+'~'
            s.send(l)
            print 'Sending done...'
            break
        s.send(l)
        print 'Sending'
    f.close()
    print "Done Sending"

    #s.shutdown(socket.SHUT_WR)
def send_fromdedupe(file_name,s,ds):
    content=ds.read(file_name)
    i=0
    print "deduplicated content "+content
    for i in range(0,len(content)+1,1024):
        if i+1024<=len(content):
            s.send(content[i:i+1024])
        else:
            s.send(content[i:]+"~")
    print "Done Sending"

def sendresponseandfile(storagepath, file_name, s, ack, ds):
    sendlib.write_socket(s, ack)
    if os.path.isfile(storagepath+file_name):
        send_fromactual(storagepath + file_name, s)
    else:
        send_fromdedupe(file_name, s, ds)




