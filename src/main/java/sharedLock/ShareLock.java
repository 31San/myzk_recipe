package sharedLock;

import org.apache.zookeeper.*;

public class ShareLock implements Watcher {

    ZooKeeper zk;
    Object mutex;
    String queue;
    String bucket;
    String sequence;
    String read;
    String write;
    String path;

    public ShareLock(ZooKeeper zooKeeper, String queue, String bucket,String read,String write){
        this.zk=zooKeeper;
        this.queue=queue;
        this.bucket=bucket;
        this.read=read;
        this.write=write;
        mutex = new Object();


    }

    public void getReadLock() throws InterruptedException, KeeperException {

        //create node
        var sequence=zk.create(queue+read,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        this.sequence=sequence;

        //simply wait
        synchronized (mutex){
            zk.getData(sequence,this,null);
            mutex.wait();
        }

    }

    public void getWriteLock() throws InterruptedException, KeeperException {
        var sequence=zk.create(queue+write,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        this.sequence=sequence;
        synchronized (mutex){
            zk.getData(sequence,this,null);
            mutex.wait();
        }
    }


    //remove both from queue and bucket
    public void unlock() throws InterruptedException, KeeperException {

        zk.delete(path,0);

        zk.delete(sequence,1);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        synchronized (mutex){
            if(watchedEvent.getType()== Event.EventType.NodeDataChanged){


                String path = bucket+"/"+sequence.substring(queue.length()+1);
                this.path=path;



                mutex.notifyAll();
            }
        }
    }
}
