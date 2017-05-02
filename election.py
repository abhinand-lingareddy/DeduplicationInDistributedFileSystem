
import kazoo
from kazoo.client import KazooState

class election:
    def stat_listener(self,state):
        if state == KazooState.LOST:
            print 'Lost'
        elif state == KazooState.SUSPENDED:
            print 'Suspended'
        elif state == KazooState.CONNECTED:
            self.perform()
            print 'Connected'
        else:
            print 'Unknown state'

    def getmaster(self):
        return self.is_master


    def __init__(self, zk,path,value):
        self.zk=zk
        self.path=path
        self.value=value
        self.is_master=False
        self.childinfo=None
        zk.add_listener(self.stat_listener)



    def find_parent(self,key):
        while(True):
            stat=self.zk.exists("parent/" + key)
            if stat==None:
                return None
            else:
                parent = self.zk.get("parent/" + key)
                key=str(parent[0])
            stat = self.zk.exists(key)
            if stat is not None:
                return key
    def get_key(self,num):
        return self.path+str(num).zfill(10)

    def watch_parent(self, data, stat):
        print "watching parent"
        if stat is None:
            print "called for parent" + str(data)
            self.parentkey = self.find_parent(self.parentkey)
            if self.parentkey is None:
                self.is_master = True
            else:
                print "parent " + self.parentkey
                kazoo.recipe.watchers.DataWatch(self.zk, self.parentkey, func=self.watch_parent)
            return False


    def watch_child(self,data, stat):
        print "watch child"
        if stat is None :
            if self.childinfo is not None:
                lock = self.zk.Lock("/lockpath", self.value)
                with lock:
                    status,child_key=self.find_child()
                    if not status:
                        self.childinfo=None
                    print "watching child"+child_key
                    kazoo.recipe.watchers.DataWatch(self.zk, child_key, func=self.watch_child)
        else:
            self.childinfo=data
            print self.childnum


    def get_num(self,key):
        return int(key[key.index(self.path) + len(self.path):])



    def find_child(self):
            print "entered find child"
            end=self.zk.get("end")
            end_num=self.get_num(str(end[0]))
            print "end int"+str(end_num)
            self.childnum=self.childnum+1
            child_key = self.get_key(self.childnum)
            while(self.childnum<=end_num):
                stat=self.zk.exists(child_key)
                if stat is not None:
                    return True,child_key
                self.childnum=self.childnum+1
                child_key = self.get_key(self.childnum)
            return False,child_key


    def perform(self):
        lock = self.zk.Lock("/lockpath", self.value)

        with lock:
            self.key=str(self.zk.create( self.path, self.value, ephemeral=True, makepath=True,sequence=True))
            print "my key"+self.key
            num=self.get_num(self.key)
            self.childnum=int(num)+1
            end=self.zk.exists("end")
            if end is None:
                self.zk.create("end",self.key)
                self.parentkey=None
            else:
                self.parentkey = str(self.zk.get("end")[0])
                self.zk.set("end", self.key)
                self.zk.create("parent/" + self.key, self.parentkey, makepath=True)


            if self.parentkey is None:
                self.is_master=True
            else:
                self.is_master = False
                kazoo.recipe.watchers.DataWatch(self.zk, self.parentkey, func=self.watch_parent)
            child_key=self.get_key(self.childnum)
            kazoo.recipe.watchers.DataWatch(self.zk,child_key , func=self.watch_child)


