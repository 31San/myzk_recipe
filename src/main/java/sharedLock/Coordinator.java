package sharedLock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.regex.Pattern;

public class Coordinator implements Watcher {
    ZooKeeper zk;
    String queue;
    String bucket;
    String read;
    String write;
    int maxOffset;

    //init two path, a queue and a bucket
    //set watch
    public Coordinator(ZooKeeper zk,String queue,String bucket,String read,String write){
        try {
            this.zk=zk;
            this.queue=queue;
            this.bucket=bucket;
            this.read=read;
            this.write=write;

            if(zk.exists(queue,false)==null){
                zk.create(queue,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            if(zk.exists(bucket,false)==null){
                zk.create(bucket,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            zk.getChildren(queue,this);
    //        zk.getChildren(bucket,this);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    //watch queue
    //if bucket empty,write the lowest node
    //if read node in bucket, write all read node. record max
    @Override
    public void process(WatchedEvent watchedEvent) {






        try {



            var bucketContent = zk.getChildren(bucket,null);

            var children = zk.getChildren(queue,null);



            if(children.size()==0){
                zk.getChildren(queue,this);
                return;
            }

            Pattern number = Pattern.compile("\\d+");


            //check bucket node type
            String type="";

            if(bucketContent.size()==0){
                //write date to the lowest node
                int min = Integer.MAX_VALUE;
                String minNode="";


                for(String str:children){
                   var matcher = number.matcher(str);
                    if (matcher.find()){
                        var child = Integer.valueOf(matcher.group());
                        if(child<min){
                            min=child;
                            minNode=str;
                        }
                    }
                }

                //copy to bucket

                zk.create(bucket+"/"+minNode,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                this.maxOffset=min;

                Stat stat = new Stat();
                zk.getData(queue+"/"+minNode,false,stat);
                zk.setData(queue+"/"+minNode,new byte[0],stat.getVersion());

                type=minNode;


            }else {
              type=bucketContent.get(0);
            }



            //judge bucket type
            Pattern letter = Pattern.compile("\\D+");
            var matcher= letter.matcher(type);
            if(matcher.find()){
                type=matcher.group();
            }

            if(type.equals(write.substring(1))){
                zk.getChildren(queue,this);
                return;
            }else {
                //read type
                //sort read node, write data to it
                int max=Integer.MIN_VALUE;

                for(int i=0;i<children.size();i++){
                    String str = children.get(i);

                    //if read node

                    if(str.contains(read.substring(1))){

                        //get sequence
                        var match = number.matcher(str);
                        match.find();
                       int value = Integer.valueOf(match.group());

                        //avoid repeated operation
                       if(value>maxOffset){


                           if(value>max){
                               max=value;
                           }

                           //copy to bucket

                           zk.create(bucket+"/"+str,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                           Stat stat = new Stat();
                           zk.getData(queue+"/"+str,false,stat);
                           zk.setData(queue+"/"+str,new byte[0],stat.getVersion());
                       }

                    }
                }

                //update maxOffset
                if(max>maxOffset){
                    maxOffset=max;
                }

            }



            zk.getChildren(queue,this);

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }


}
