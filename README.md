# myzk_recipe

Common distributed data structure implementation based on Zookeeper, including mutex and share lock currently.

## How share lock with coordinator works?


Coordinator class init 2 base path queue and bucket, then set watcher on children of queue.

each time watcher was trigger:

1 if bucket empty,  calls setData on lowest node 

2 if bucket is in read state, call setData on other read node



ShareLock class create a read or write Node on queue while getting lock , and watches its data change. 

After unlock method called, it removes both node in queue and bucket.




