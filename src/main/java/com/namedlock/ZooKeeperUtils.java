package com.namedlock;

import com.google.common.collect.ImmutableList;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.List;

/**
 * Utilities for dealing with zoo keeper.
 */
public final class ZooKeeperUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperUtils.class.getName());

    /**
     * The magic version number that allows any mutation to always succeed regardless of actual
     * version number.
     */
    public static final int ANY_VERSION = -1;

    /**
     * An ACL that gives all permissions any user authenticated or not.
     */
    public static final ImmutableList<ACL> OPEN_ACL_UNSAFE =
            ImmutableList.copyOf(ZooDefs.Ids.OPEN_ACL_UNSAFE);

    /**
     * An ACL that gives all permissions to node creators and read permissions only to everyone else.
     */
    public static final ImmutableList<ACL> EVERYONE_READ_CREATOR_ALL =
            ImmutableList.<ACL>builder()
                    .addAll(ZooDefs.Ids.CREATOR_ALL_ACL)
                    .addAll(ZooDefs.Ids.READ_ACL_UNSAFE)
                    .build();

    /**
     * Returns true if the given exception indicates an error that can be resolved by retrying the
     * operation without modification.
     *
     * @param e the exception to check
     * @return true if the causing operation is strictly retryable
     */
    public static boolean isRetryable(KeeperException e) {
        Assert.notNull(e, "");

        switch (e.code()) {
            case CONNECTIONLOSS:
            case SESSIONEXPIRED:
            case SESSIONMOVED:
            case OPERATIONTIMEOUT:
                return true;

            case RUNTIMEINCONSISTENCY:
            case DATAINCONSISTENCY:
            case MARSHALLINGERROR:
            case BADARGUMENTS:
            case NONODE:
            case NOAUTH:
            case BADVERSION:
            case NOCHILDRENFOREPHEMERALS:
            case NODEEXISTS:
            case NOTEMPTY:
            case INVALIDCALLBACK:
            case INVALIDACL:
            case AUTHFAILED:
            case UNIMPLEMENTED:

                // These two should not be encountered - they are used internally by ZK to specify ranges
            case SYSTEMERROR:
            case APIERROR:

            case OK: // This is actually an invalid ZK exception code

            default:
                return false;
        }
    }

    /**
     * Ensures the given {@code path} exists in the ZK cluster accessed by {@code zkClient}.  If the
     * path already exists, nothing is done; however if any portion of the path is missing, it will be
     * created with the given {@code acl} as a persistent zookeeper node.  The given {@code path} must
     * be a valid zookeeper absolute path.
     *
     * @param zkClient the client to use to access the ZK cluster
     * @param acl      the acl to use if creating path nodes
     * @param path     the path to ensure exists
     * @throws InterruptedException if we were interrupted attempting to connect to the ZK cluster
     * @throws KeeperException      if there was a problem in ZK
     */
    public static void ensurePath(ZkClient zkClient, List<ACL> acl, String path)
            throws InterruptedException, KeeperException {

        Assert.notNull(zkClient, "zkClient must not be null.");
        Assert.notNull(path, "path must not be null.");
        Assert.isTrue(path.startsWith("/"), "");

        ensurePathInternal(zkClient, acl, path);
    }

    private static void ensurePathInternal(ZkClient zkClient, List<ACL> acl, String path)
            throws InterruptedException, KeeperException {
        if (!zkClient.exists(path)) {
            // The current path does not exist; so back up a level and ensure the parent path exists
            // unless we're already a root-level path.
            int lastPathIndex = path.lastIndexOf('/');
            if (lastPathIndex > 0) {
                ensurePathInternal(zkClient, acl, path.substring(0, lastPathIndex));
            }

            // We've ensured our parent path (if any) exists so we can proceed to create our path.
            zkClient.create(path, null, CreateMode.PERSISTENT);

        }
    }

    /**
     * Validate and return a normalized zookeeper path which doesn't contain consecutive slashes and
     * never ends with a slash (except for root path).
     *
     * @param path the path to be normalized
     * @return normalized path string
     */
    public static String normalizePath(String path) {
        String normalizedPath = path.replaceAll("//+", "/").replaceFirst("(.+)/$", "$1");
        PathUtils.validatePath(normalizedPath);
        return normalizedPath;
    }

    private ZooKeeperUtils() {
        // utility
    }
}