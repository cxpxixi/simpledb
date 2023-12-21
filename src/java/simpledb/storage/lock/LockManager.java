package simpledb.storage.lock;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private Map<PageId,Map<TransactionId,PageLock>> PageLockMap;
    public LockManager(){
        this.PageLockMap=new ConcurrentHashMap<>();
    }
    /**
     * LockManager来实现对锁的管理，LockManager中主要有申请锁、释放锁、查看指定数据页的指定事务是否有锁这三个功能
     */
    public synchronized boolean acquireLock(PageId pageId, TransactionId tid, int acquireType) throws TransactionAbortedException, InterruptedException{
        String lockType = acquireType == 0 ? "read lock" : "write lock";
        String threadName = Thread.currentThread().getName();
        Map<TransactionId, PageLock> lockMap = PageLockMap.get(pageId);
        if (lockMap==null||lockMap.size()==0){
            PageLock pageLock = new PageLock(acquireType, tid);
            lockMap=new ConcurrentHashMap<>();
            lockMap.put(tid,pageLock);
            PageLockMap.put(pageId,lockMap);
            return true;
        }
        PageLock pageLock = lockMap.get(tid);
        if (pageLock!=null){
            if (pageLock.getType()==PageLock.SHARE) {
                if (acquireType == PageLock.SHARE) {
                    System.out.println(threadName + ": the" + pageId + "have read lock with the same tid, transaction " + tid + " require" + lockType + " success");
                    return true;
                }
                if (acquireType==PageLock.EXCLUSIVE) {
                    if (lockMap.size() > 1) {
                        System.out.println(threadName + ": the" + pageId + "have many read locks, transaction " + tid + " require" + lockType + " fail");
                        throw new TransactionAbortedException();
                    }
                    if (lockMap.size() == 1) {
                        pageLock.setType(PageLock.EXCLUSIVE);
                        lockMap.put(tid, pageLock);
                        PageLockMap.put(pageId, lockMap);
                        return true;
                    }
                }
            }
            if (pageLock.getType()==PageLock.EXCLUSIVE){
                return true;
            }
        }

        if (pageLock==null){
            if (lockMap.size()>1){
                if (acquireType==PageLock.SHARE){
                    pageLock=new PageLock(PageLock.SHARE,tid);
                    lockMap.put(tid,pageLock);
                    PageLockMap.put(pageId,lockMap);
                    return true;
                }
                if (acquireType==PageLock.EXCLUSIVE){
                    wait(10);
                    return false;
                }
            }
            if (lockMap.size()==1){
                PageLock curLock = null;
                for (PageLock lock : lockMap.values()){
                    curLock = lock;
                }
                if (curLock.getType() == PageLock.SHARE){
                    //如果是读锁
                    if (acquireType == PageLock.SHARE){
                        // tid 需要获取的是读锁
                        pageLock = new PageLock(PageLock.SHARE,tid);
                        lockMap.put(tid,pageLock);
                        PageLockMap.put(pageId,lockMap);
                        return true;
                    }
                    if (acquireType == PageLock.EXCLUSIVE){
                        // tid 需要获取写锁
                        wait(10);
                        return false;
                    }
                }
                if (curLock.getType() == PageLock.EXCLUSIVE){
                    // 如果是写锁
                    wait(10);
                    return false;
                }
            }
        }
        return false;
    }

    public synchronized void releaseLock(PageId pageId, TransactionId tid){
        Map<TransactionId, PageLock> lockMap = PageLockMap.get(pageId);
        if (lockMap==null)
        {
            return;
        }
        if (tid==null)
        {
            return;
        }
        PageLock pageLock = lockMap.get(tid);
        if (pageLock==null){
            return;
        }
        lockMap.remove(tid);
        if (lockMap.size()==0){
            PageLockMap.remove(pageId);
        }
        this.notifyAll();
    }

    public synchronized boolean isHoldLock(PageId pageId, TransactionId tid){
        Map<TransactionId, PageLock> lockMap = PageLockMap.get(pageId);
        if (lockMap==null)
        {
            return false;
        }
        return lockMap.get(tid)!=null;
    }

    public synchronized void completeTransaction(TransactionId tid) {
        Set<PageId> pageIds = PageLockMap.keySet();
        for (PageId pageId : pageIds) {
            releaseLock(pageId, tid);
        }
    }
}
