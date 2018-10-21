package com.namedlock;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@ContextConfiguration("classpath:/zk-context.xml")
public class DistributedLockTest {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Resource
    private ZkClient zkClient;

    @Resource
    private DistributedLock distributedLock;

    @Test
    public void assertZkNotNull() {
        Assert.assertNotNull(zkClient);
        Assert.assertNotNull(distributedLock);
    }

    @Test
    public void testLock() {
        try {
            distributedLock.lock();
        } catch (Exception e) {
            Assert.fail();
        } finally {
            distributedLock.unlock();
        }
    }

    @Test
    public void testDoubleUnlock() {
        try {
            distributedLock.lock();
        } catch (Exception e) {
            Assert.fail();
        } finally {
            distributedLock.unlock();
            distributedLock.unlock();
        }

        try {
            distributedLock.lock();
        } catch (Exception e) {
            Assert.fail();
        } finally {
            distributedLock.unlock();
        }
    }

    @Test
    public void testDoubleLock() {
        try {
            distributedLock.lock();
            distributedLock.lock();
            Assert.fail();
        } catch (Exception e) {
            logger.warn("expected to be here", e);
        } finally {
            distributedLock.unlock();
        }
    }


    @Test
    public void testTryLock() {
        try {
            boolean locked = distributedLock.tryLock(1000, TimeUnit.MILLISECONDS);
            Assert.assertTrue(locked);
        } catch (Exception e) {
            Assert.fail();
        } finally {
            distributedLock.unlock();
        }
    }

    @Test
    public void testDoubleTryLock() {
        try {
            boolean success = distributedLock.tryLock(1000, TimeUnit.MILLISECONDS);
            Assert.assertTrue(success);
            distributedLock.tryLock(1000, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (Exception e) {
        } finally {
            distributedLock.unlock();
        }
    }


    @Test
    public void testMThread() throws InterruptedException {
        for (int i = 0; i < countDownLatchNum; i ++){
            new MThreadLock().start();
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        TimeUnit.SECONDS.sleep(countDownLatchNum+1);
        Assert.assertTrue(count <= countDownLatchNum);
    }

    private static int countDownLatchNum = 8;
    private static int count = 0;
    private static CountDownLatch countDownLatch = new CountDownLatch(countDownLatchNum);
    private class MThreadLock extends Thread{

        private DistributedLock distributedLock = new DistributedLockImpl(zkClient, "/test/lock/mthread/tryx");
        @Override
        public void run() {
            try {
                if(distributedLock.tryLock(3, TimeUnit.SECONDS)){
                    count++;
                    TimeUnit.MILLISECONDS.sleep(10);
                }else {
                    logger.info("try lock failed.");
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
                distributedLock.unlock();
            }
            logger.info("run finished.");
        }
    }


}
