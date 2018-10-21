package com.namedlock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author tonyzhi
 * mainly gotten from twitter.common @see https://github.com/twitter/commons/blob/master/src/java/com/twitter/common/zookeeper/DistributedLockImpl.java
 */
public class DistributedLockImpl implements DistributedLock {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedLockImpl.class.getName());

    private final ZkClient zkClient;
    private final String lockPath;
    private final ImmutableList<ACL> acl;

    private CountDownLatch syncPoint;
    private boolean holdsLock = false;
    private String currentId;
    private String currentNode;
    private String watchedNode;
    private ChildPathListener childPathListener = new ChildPathListener();

    /**
     * Equivalent to {@link #DistributedLockImpl(ZkClient, String, Iterable)} with a default
     * wide open {@code acl} ({@link ZooDefs.Ids#OPEN_ACL_UNSAFE}).
     */
    public DistributedLockImpl(ZkClient zkClient, String lockPath) {
        this(zkClient, lockPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    /**
     * Creates a distributed lock using the given {@code zkClient} to coordinate locking.
     *
     * @param zkClient The ZooKeeper client to use.
     * @param lockPath The path used to manage the lock under.
     * @param acl      The acl to apply to newly created lock nodes.
     */
    public DistributedLockImpl(ZkClient zkClient, String lockPath, Iterable<ACL> acl) {
        Assert.notNull(zkClient, "zkClient must not be null");
        this.zkClient = zkClient;
        Assert.notNull(lockPath, "lock path must not be empty");
        this.lockPath = lockPath;
        this.acl = ImmutableList.copyOf(acl);
        this.syncPoint = new CountDownLatch(1);
    }

    private synchronized void prepare()
            throws InterruptedException, KeeperException {

        try {
            ZooKeeperUtils.ensurePath(zkClient, acl, lockPath);
        } catch (KeeperException.NodeExistsException e) {
            LOG.warn("it is ok.", e);
        }

        // Create an EPHEMERAL_SEQUENTIAL node.
        currentNode =
                zkClient.create(lockPath + "/member_", null, acl, CreateMode.EPHEMERAL_SEQUENTIAL);

        // We only care about our actual id since we want to compare ourselves to siblings.
        if (currentNode.contains("/")) {
            currentId = currentNode.substring(currentNode.lastIndexOf("/") + 1);
        }
        LOG.debug("Received ID from zk:" + currentId);
    }

    @Override
    public synchronized void lock() throws LockingException {
        if (holdsLock) {
            throw new LockingException("Error, already holding a lock. Call unlock first!");
        }
        try {
            prepare();
            childPathListener.checkForLock();
            syncPoint.await();
            if (!holdsLock) {
                throw new LockingException("Error, couldn't acquire the lock!");
            }
        } catch (InterruptedException e) {
            cancelAttempt();
            throw new LockingException("InterruptedException while trying to acquire lock!", e);
        } catch (KeeperException e) {
            // No need to clean up since the node wasn't created yet.
            throw new LockingException("KeeperException while trying to acquire lock!", e);
        }
    }

    @Override
    public synchronized boolean tryLock(long timeout, TimeUnit unit) {
        if (holdsLock) {
            throw new LockingException("Error, already holding a lock. Call unlock first!");
        }
        try {
            prepare();
            childPathListener.checkForLock();
            boolean success = syncPoint.await(timeout, unit);
            if (!success) {
                return false;
            }
            if (!holdsLock) {
                throw new LockingException("Error, couldn't acquire the lock!");
            }
        } catch (InterruptedException e) {
            cancelAttempt();
            return false;
        } catch (KeeperException e) {
            // No need to clean up since the node wasn't created yet.
            throw new LockingException("KeeperException while trying to acquire lock!", e);
        }
        return true;
    }

    @Override
    public synchronized void unlock() throws LockingException {
        if (currentId == null) {
            return;
        }
        LOG.debug("Cleaning up this locks ephemeral node.");
        cleanup();
    }

    //TODO(Florian Leibert): Make sure this isn't a runtime exception. Put exceptions into the token?

    private synchronized void cancelAttempt() {
        LOG.info("Cancelling lock attempt!");
        cleanup();
        holdsLock = false;
        syncPoint.countDown();
    }

    private void cleanup() {
        LOG.info("Cleaning up!");
        if(!StringUtils.isEmpty(currentId)){
            try {
                boolean exists = zkClient.exists(currentNode);
                if (exists) {
                    zkClient.delete(currentNode);
                    LOG.info("deleted node:"+currentNode);
                } else {
                    LOG.warn("Called cleanup but nothing to cleanup!");
                }
            } catch (Exception e) {
                LOG.warn("error when deleting "+currentId, e);
            }
        }

        LOG.info(String.format("node [%s] released the lock." , currentId));

        holdsLock = false;
        currentId = null;
        currentNode = null;
        syncPoint = new CountDownLatch(1);
    }

    class ChildPathListener implements IZkChildListener{

        public synchronized void checkForLock() {
            Assert.notNull(currentId, "currentId must not be null.");

            List<String> candidates = zkClient.getChildren(lockPath);
            ImmutableList<String> sortedMembers = Ordering.natural().immutableSortedCopy(candidates);

            // Unexpected behavior if there are no children!
            if (sortedMembers.isEmpty()) {
                throw new LockingException("Error, member list is empty!");
            }

            int memberIndex = sortedMembers.indexOf(currentId);

            // If we hold the lock
            if (memberIndex == 0) {
                holdsLock = true;
                syncPoint.countDown();
                LOG.info(String.format("node [%s] got the lock." , currentId));
            } else {
                final String nextLowestNode = sortedMembers.get(memberIndex - 1);
                LOG.info(String.format("Current LockWatcher with ephemeral node [%s], is " +
                        "waiting for [%s] to release lock.", currentId, nextLowestNode));

                watchedNode = String.format("%s/%s", lockPath, nextLowestNode);
                boolean exists = zkClient.exists(watchedNode);
                if (!exists) {
                    checkForLock();
                }
                zkClient.subscribeChildChanges(lockPath, this);
            }

        }


        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            if(currentId==null){
                return;
            }
            checkForLock();
        }
    }

}