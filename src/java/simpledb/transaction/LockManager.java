package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private static final Byte FK = 0;
    private static final int MINTIME = 100, MAXLEN = 900;
    private final Random random = new Random();

    private final Map<PageId, Map<TransactionId, Byte>> pageReadHolders;
    private final Map<PageId, TransactionId> pageWriteHolders;

    public LockManager(){
        this.pageReadHolders = new ConcurrentHashMap<PageId, Map<TransactionId, Byte>>();
        this.pageWriteHolders = new ConcurrentHashMap<PageId, TransactionId>();
    }

    /**
     * 事物tid尝试对页pid上锁, p指出是读锁还是写锁
     * @param tid
     * @param pid
     * @param p
     */
    public void acquire(TransactionId tid, PageId pid, Permissions p) throws TransactionAbortedException{
        try {
            if(p == Permissions.READ_ONLY) {
                acquireReadLock(tid, pid);
            } else {
                acquireWriteLock(tid, pid);
            }
        } catch (InterruptedException e) {
            throw new TransactionAbortedException();
        }
    }

    /**
     * 事物tid尝试对页pid上读锁
     * @param tid
     * @param pid
     */
    public void acquireReadLock(TransactionId tid, PageId pid) throws InterruptedException {
        if(!hold(tid, pid)) {
            synchronized (pid) {
                Thread thread = Thread.currentThread();
                Timer timer = new Timer(true);
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        thread.interrupt();
                    }
                }, MINTIME + random.nextInt(MAXLEN));
                while(pageWriteHolders.containsKey(pid)) {
                    pid.wait(5);
                }
                timer.cancel();
                pageReadHolders.computeIfAbsent(pid, key-> new ConcurrentHashMap<TransactionId, Byte>()).put(tid, FK);
            }
        }
    }

    /**
     * 事物tid尝试对页pid上写锁
     * @param tid
     * @param pid
     */
    public void acquireWriteLock(TransactionId tid, PageId pid) throws InterruptedException{
        if(!holdWriteLock(tid, pid)) {
            synchronized (pid) {
                Thread thread = new Thread();
                Timer timer = new Timer(true);
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        thread.interrupt();
                    }
                }, MINTIME + random.nextInt(MAXLEN));
                while(haveOtherReadersOnPage(tid, pid) || pageWriteHolders.containsKey(pid)) {
                    pid.wait(5);
                }
                timer.cancel();
                pageWriteHolders.put(pid, tid);
            }
        }
    }

    /**
     * 事物tid是否有页pid上的锁
     * @param tid
     * @param pid
     * @return
     */
    public boolean hold(TransactionId tid, PageId pid) {
        return holdReadLock(tid, pid) || holdWriteLock(tid, pid);
    }

    /**
     * 事物tid是否有页pid上的读锁
     * @param pid
     * @return
     */
    public boolean holdReadLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            return pageReadHolders.containsKey(pid) && pageReadHolders.get(pid).containsKey(tid);
        }
    }

    /**
     * 事物tid是否有页pid上的写锁
     * @param tid
     * @param pid
     * @return
     */
    public boolean holdWriteLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            return pageWriteHolders.containsKey(pid) && pageWriteHolders.get(pid).equals(tid);
        }
    }

    /**
     * 判断页面pid上是否有除了事物tid外其他的读锁
     * @param tid
     * @param pid
     * @return
     */
    public boolean haveOtherReadersOnPage(TransactionId tid, PageId pid) {
        synchronized (pid) {
            if(pageReadHolders.containsKey(pid)) {
                for(TransactionId t : pageReadHolders.get(pid).keySet()) {
                    if(!t.equals(tid)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /**
     * 释放事物tid在页面pid上的所有锁
     * @param tid
     * @param pid
     */
    public void release(TransactionId tid, PageId pid) {
        releaseReadLock(tid, pid);
        releaseWriteLock(tid, pid);
    }

    /**
     * 删除事物tid在页面pid上的读锁
     * @param tid
     * @param pid
     */
    public void releaseReadLock(TransactionId tid, PageId pid) {
        if(holdReadLock(tid, pid)) {
            synchronized (pid) {
                pageReadHolders.get(pid).remove(tid);
                if(pageReadHolders.get(pid).isEmpty()) {
                    pageReadHolders.remove(pid);
                }
            }
        }
    }

    /**
     * 删除事物tid在页面pid上的写锁
     * @param tid
     * @param pid
     */
    public void releaseWriteLock(TransactionId tid, PageId pid) {
        if(holdWriteLock(tid, pid)) {
            synchronized (pid) {
                pageWriteHolders.remove(pid);
            }
        }
    }

    /**
     * 释放事物tid在所有页面上的锁
     * @param tid
     */
    public void releaseAll(TransactionId tid) {
        for(PageId pid : pageWriteHolders.keySet()) {
            releaseWriteLock(tid, pid);
        }
        for(PageId pid : pageReadHolders.keySet()) {
            releaseReadLock(tid, pid);
        }
     }

}