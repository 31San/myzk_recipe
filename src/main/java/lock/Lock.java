package lock;

import org.apache.zookeeper.*;

import java.util.ArrayList;
import java.util.Comparator;

import static org.apache.zookeeper.Watcher.Event.EventType.NodeDeleted;


//implement watch of lower node, for notify current thread
public class Lock implements Watcher {

    ZooKeeper zk;
    Object mutex;
    String basePath;
    String suffix;
    String sequence;

    //set server
    //create lock path if not exist
    //init a mutex
    public Lock(ZooKeeper zooKeeper, String basePath, String suffix){

        this.zk=zooKeeper;
        this.basePath=basePath;
        this.suffix=suffix;
        mutex = new Object();


    }


    //check basePath exist
    //create lock temp node
    //call getChildren
    //check sequence, if lowest, acquire lock
    //else call exist to set watch on next lower node
    //wait for watch to notify

    //in case of watched node disappear, check lowest in loop
    public void getLock() throws InterruptedException, KeeperException {

        //check root
        if(zk.exists(basePath,false)==null){
            zk.create(basePath,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        //create node
        //what is current sequence?
        var sequence=zk.create(basePath+suffix,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);

        sequence=sequence.substring(basePath.length()+1);
        this.sequence=sequence;

        //while true loop in synchronize mutex

        while (true){
           synchronized (mutex){

               var children = zk.getChildren(basePath,false);

               //sort children acs

               int length =suffix.length()-1;

               Comparator<String> stringComparator=(s1,s2)->{
                  var  v1=Integer.valueOf( s1.substring(length));
                  var v2 = Integer.valueOf(s2.substring(length));
                  return  v1-v2;
               };

               children.sort(stringComparator);

               if(sequence.equals(children.get(0))){
                   return;
               }else {

                   int i=0;
                   for(;i<children.size();i++){

                       if(sequence.equals(children.get(i))){
                           break;
                       }
                   }


                   //set watch on lower node, if watch does not exist yet
                   //wait
                    String watchPath=basePath+"/"+children.get(i-1);
                   zk.exists(watchPath,this);
                   mutex.wait();
               }


           }
        }





    }

    //delete node
    public void unLock() throws InterruptedException, KeeperException {
        //how to delete temp node?
        zk.delete(basePath+"/"+sequence,0);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        synchronized (mutex){
            if(watchedEvent.getType()==NodeDeleted){
                mutex.notifyAll();
            }
        }

    }
}
