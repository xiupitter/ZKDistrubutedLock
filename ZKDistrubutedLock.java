package simpleDistLockTest;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.SortedSet;
import java.util.TreeSet;
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

public class ZKDistrubutedLock implements Watcher{

  private ZooKeeper zk;
	
	private String keyLocked;
	private String node;
	private boolean isInnterInterrupt = false;

	private Thread currentLockThread;
	private Watcher defaultWatcher;
	
	public ZKDistrubutedLock(String key) {
		// TODO Auto-generated constructor stub
		this.keyLocked ="/" +key;
		defaultWatcher = new Watcher() {
					
					@Override
					public void process(WatchedEvent event) {
						// TODO Auto-generated method stub
						if(event.getState().equals(Event.KeeperState.SyncConnected)){
							if(event.getPath().contains(keyLocked)&&event.getType().equals(Event.EventType.NodeDeleted)){
								try {
									SortedSet<String> set = new TreeSet<String>(zk.getChildren(keyLocked, false));
									if(node.equals(keyLocked+"/"+set.first())){
										isInnterInterrupt = true;
										currentLockThread.interrupt();
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
				};
		try {
			zk = new ZooKeeper("127.0.0.1:2181", 5*1000, this);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void lockInterruptibly() throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		currentLockThread= Thread.currentThread();
		node = zk.create(keyLocked+"/lock", (" ").getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		SortedSet<String> set = new TreeSet<String>(zk.getChildren(keyLocked, false));
		if(node.equals(keyLocked+"/"+set.first())){//success
			return;
		}else{//fail
			try{
				zk.exists(keyLocked+"/"+set.first(),defaultWatcher);
				Thread.sleep(1000*1000);
			}catch(InterruptedException e){
				if(!isInnterInterrupt){
					throw e;
				}else{
					isInnterInterrupt =false;
				}
			}
		}
	}

	public boolean tryLock() throws KeeperException, InterruptedException {
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

	public boolean tryLock(long time, TimeUnit unit)
			throws InterruptedException, KeeperException {
		// TODO Auto-generated method stub
		
		currentLockThread= Thread.currentThread();
		node = zk.create(keyLocked+"/lock", (" ").getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		SortedSet<String> set = new TreeSet<String>(zk.getChildren(keyLocked, false));
		if(node.equals(keyLocked+"/"+set.first())){//success
			return true;
		}else{//fail
			try{
				zk.exists(keyLocked+"/"+set.first(),defaultWatcher);
				this.wait(unit.toMillis(time));
			}catch(InterruptedException e){
				if(!isInnterInterrupt){
					throw e;
				}else{
					isInnterInterrupt =false;
				}
			}
		}
		return false;
	}

	public void unlock() throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		zk.close();
	}

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		if(event.getState().equals(Event.KeeperState.SyncConnected)){
			try {
				if(zk!=null){
					if(zk.exists(keyLocked, true)==null){
						zk.create(keyLocked, "a list of locked client on lock a".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					}
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
