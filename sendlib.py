import pickle

def no_to_bytes(no):
    b=str(no)
    non_zeroscounter=len(b)
    zeros=[]
    for i in range(12-non_zeroscounter):
        zeros.append("0")
    return "".join(zeros)+b


def bytes_to_no(b):
    return int(b)


def read_socket(soc):
    length=soc.recv(12)
    print "length "+str(length)
    if(len(length)==0):
        return None
    length=bytes_to_no(length)

    print "length in number "+str(length)



    i=0
    request=[]
    while(i<length):
        if(length-i>512):
            bufflength=512
        else:
            bufflength=length-i
        st=soc.recv(bufflength)
        #print "st "+st
        st = pickle.loads(st)
        request.append(st)
        i=i+bufflength

    st="".join(request)
    # st = pickle.loads(soc.recv(12000))
    # print "read socket"+st
    # st=st.encode("utf-8")

    return st

def write_socket(soc, response):
    response = pickle.dumps(response)
    length=len(response)
    length_bytes=no_to_bytes(length)
    print "length "+str(length)+" length bytes "+length_bytes
    soc.send(length_bytes)
    i=0
    while(i<length):
        if length-i>512:
            bufflength=512
        else:
            bufflength=length-i
        st=response[i:i + bufflength]
        print "now sending "+st
        soc.send(st)
        i=i+bufflength
    print "write socket" + response
    # soc.send(pickle.dumps(response))