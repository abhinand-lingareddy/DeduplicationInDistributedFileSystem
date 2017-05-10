# Distributed File System with Deduplication

<h2><strong>Instructions to run</strong></h2>
<p><strong>To start the server</strong></p>
<p>python masterserver.py &lt;zookeeperhostname&gt; &lt;serverport&gt; &lt;ispersistantdata&gt;</p>
<p><strong>Options</strong></p>
<p><em>zookeeperhostname</em> -&nbsp; The host name of the zookeeper to which this server is to be added</p>
<p><em>serverport&lt;optional&gt;</em>- The port number of this server. We are also creating a folder with the port number for storing the data so that multiple servers can be created in the same machine. when not specified we generate a random number for the port</p>
<p><em>ispersistantdata</em>&lt;optional&gt;- To persist the data/metadata. by default false. we just tested basic&nbsp;<span data-dobid="hdw">scenario</span> with true. When using true, port number is required to specify the data directory.</p>
<p>&nbsp;</p>
<p><strong>To start FUSE client</strong></p>
<p>python myfuse.py &lt;zookeeperhostname&gt; &lt;mountpoint&gt;</p>
<p><em>zookeeperhostname</em> -&nbsp; The host name of the zookeeper from which the server infomation can be fetched.</p>
<p>mountpoint- mount point for fuse</p>
<p>&nbsp;</p>
<p><strong>To run stress test</strong></p>
<p>python stresstest2.py &lt;zookeeperhostname&gt; &lt;filename&gt;</p>
<p>creates 10 files each using 10 clients parallely. (In total creates 100 files).</p>
<p>&nbsp;</p>
<p><a href="https://drive.google.com/file/d/0B_vtXHp40F8DMmRIRkw4Uzc3OW5HRU1SLVN6amZ0NHdxSFVR/view?usp=sharing">Read more about this system</a></p>
<p><a href="https://drive.google.com/file/d/0B_vtXHp40F8DMl9oNThVckxmRjVldWlUdjF3WTQxWTMtTFhN/view?usp=sharing">presentation</a></p>
