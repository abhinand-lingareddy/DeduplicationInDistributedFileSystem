from kazoo.client import KazooClient

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
zk.delete("dedupequeue",recursive=True)
zk.create("dedupequeue","somevalue")