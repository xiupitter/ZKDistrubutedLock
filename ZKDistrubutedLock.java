package simpleDistLockTest;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * 一个基于zookeeper的分布式锁
 * @author Xiupitter
 *
 */
public class ZKDistrubutedLock implements Watcher{

	private ZooKeeper zk;
	
	private String keyLocked;
	private String node;
	private Watcher defaultWatcher;
	private CountDownLatch connectLock= new CountDownLatch(1); 
	private CountDownLatch lock= new CountDownLatch(1); 

	/**
	 * the key to lock
	 * @param key
	 */
	public ZKDistrubutedLock(String key) {
		// TODO Auto-generated constructor stub
		this.keyLocked ="/" +key;
		defaultWatcher = new LockWatcher();
		try {
			zk = new ZooKeeper("127.0.0.1:2181", 5*1000, this);
			connectLock.await();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public synchronized void lockInterruptibly() throws KeeperException, InterruptedException {
		node = zk.create(keyLocked+"/lock", (" ").getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		SortedSet<String> set = new TreeSet<String>(zk.getChildren(keyLocked, defaultWatcher));
		if(node.equals(keyLocked+"/"+set.first())){//success
		}else{//fail
			lock.await();
		}
		return;

	}

	public synchronized boolean tryLock() throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		node = zk.create(keyLocked+"/lock", (" ").getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		SortedSet<String> set = new TreeSet<String>(zk.getChildren(keyLocked, false));
		if(node.equals(keyLocked+"/"+set.first())){//success
			zk.delete(node, -1);
			return true;
		}else{
			zk.delete(node, -1);
			return false;
		}
	}

	public synchronized boolean tryLock(long timeout, TimeUnit unit)
			throws InterruptedException, KeeperException {
		// TODO Auto-generated method stub
		node = zk.create(keyLocked+"/lock", (" ").getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		SortedSet<String> set = new TreeSet<String>(zk.getChildren(keyLocked, defaultWatcher));
		if(node.equals(keyLocked+"/"+set.first())){//success
			return true;
		}else{//fail
			lock.await(timeout, unit);
		}
		return false;
	}

	public synchronized void unlock() throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		zk.delete(node, -1);
		zk.close();
	}

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		if(event.getState().equals(Event.KeeperState.SyncConnected)){
			try {
				if(zk!=null){
					if(zk.exists(keyLocked, false)==null){
						zk.create(keyLocked, "a lock under the root".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}
				}
				connectLock.countDown();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private class LockWatcher implements Watcher{

		@Override
		public void process(WatchedEvent event) {
			// TODO Auto-generated method stub
			if(event.getState().equals(Event.KeeperState.SyncConnected)){
				if(event.getPath().contains(keyLocked)&&event.getType().equals(Event.EventType.NodeChildrenChanged)){
					try {
						SortedSet<String> set = new TreeSet<String>(zk.getChildren(keyLocked, defaultWatcher));
						System.out.println(keyLocked+"/"+set.first()+" "+node);
						if(node.equals(keyLocked+"/"+set.first())){
							lock.countDown();
						}
					} catch (KeeperException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		
	}
}
