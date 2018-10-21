package com.namedlock;

import java.util.concurrent.TimeUnit;

/**
 * @author tonyzhi
 * A distributed lock implemation
 */
public interface DistributedLock {

    /**
     * 1. distributed lock a global resource, throw a LockingException if failed. </br>
     * 2. if the lock has been held or being interrupted, then a LockingException will also be thrown.
     * @throws LockingException
     */
    void lock() throws LockingException;

    /**
     * try to get a lock within a time limit
     * @param timeout
     * @param unit
     * @return return true if the lock was successfully acquired, else return false.
     */
    boolean tryLock(long timeout, TimeUnit unit);

    /**
     * unlock the resource
     * @throws LockingException
     */
    void unlock() throws LockingException;

    public static class LockingException extends RuntimeException {
        public LockingException(String msg, Exception e) {
            super(msg, e);
        }

        public LockingException(String msg) {
            super(msg);
        }
    }
}