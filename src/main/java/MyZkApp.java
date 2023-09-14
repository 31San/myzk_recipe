import lock.Lock;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import sharedLock.Coordinator;
import sharedLock.ShareLock;

import java.io.IOException;
import java.util.Random;

public class MyZkApp {

    public static void main(String[] args){
        System.out.println("ready");

        //creat test method for each
        //  lockTest();


        shareLockTest();

    }


    public static void   lockTest(){
        for(int i=0;i<10;i++){
            new Thread(()->{
                try {
                    ZooKeeper zk = new ZooKeeper("localhost:2181",3000,null);

                    Lock lock = new Lock(zk,"/lock","/guid");
                    lock.getLock();

                    System.out.println(Thread.currentThread().getId());

                    Thread.sleep(1000);

                    System.out.println(Thread.currentThread().getId()+"my moment ended");

                    lock.unLock();


                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    public static void shareLockTest(){

        try {


            String queue = "/queue";

            String bucket = "/bucket";

            String read = "/read";

            String write = "/write";


            //init coordinator
            {
                ZooKeeper zk = new ZooKeeper("localhost:2181",3000,null);
                Coordinator coordinator = new Coordinator(zk,queue,bucket,read,write);
                Thread.sleep(2000);
            }



            Runnable readTest = ()->{



                try {
                    ZooKeeper zk = new ZooKeeper("localhost:2181",3000,null);
                    var shareLock = new ShareLock(zk,queue,bucket,read,write);

                    shareLock.getReadLock();

                    System.out.println(Thread.currentThread().getId()+" reading ");

                    Thread.sleep(new Random().nextInt(3*1000));

                    System.out.println(Thread.currentThread().getId()+" exit");

                    shareLock.unlock();

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }


            };

            Runnable writeTest = ()->{


                try {
                    ZooKeeper zk = new ZooKeeper("localhost:2181",3000,null);
                    var shareLock = new ShareLock(zk,queue,bucket,read,write);

                    shareLock.getWriteLock();

                    System.out.print(Thread.currentThread().getId()+" ( writing ");

                    Thread.sleep(new Random().nextInt(3*1000));

                    System.out.println("        "+Thread.currentThread().getId()+" exit )");

                    shareLock.unlock();

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            };

            Random random = new Random();



            for (int i=0;i<50;i++){

                Thread.sleep(random.nextInt(1000));

                int seed = random.nextInt();

                if(seed%2==0){
                    new Thread(readTest).start();
                }else {
                    new Thread(writeTest).start();
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }



    }

}
