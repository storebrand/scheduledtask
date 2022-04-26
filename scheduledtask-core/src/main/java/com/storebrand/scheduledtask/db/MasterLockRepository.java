package com.storebrand.scheduledtask.db;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import com.storebrand.scheduledtask.ScheduledTaskRegistry.MasterLock;
import com.storebrand.scheduledtask.ScheduledTaskRegistryImpl;

/**
 * Internal repository used by {@link ScheduledTaskRegistryImpl} to handle master election for a node. An implementation
 * of this interface is required for managing master locks. This repository is only for internal use, and should not
 * be used outside scheduledtask-core, except for providing implementations.
 * <p>
 * On each {@link #tryAcquireLock(String, String)} it will also try to insert the lock, if the node managed to insert it
 * then that node has the lock.
 * <p>
 * After a lock has been acquired for a node it has to do the {@link #keepLock(String, String)} within the next 5 min
 * in order to be allowed to keep it. If it does not update withing that timespan it has to wait until the
 * {@link MasterLock#getLockLastUpdatedTime()} is over 10 min old before any node can acquire it again. This means
 * there is a 5 min gap where no node can aquire the lock at all.
 *
 * @author Dag Bertelsen - dag.lennart.bertelsen@storebrand.no - dabe@dagbertelsen.com - 2021.03
 * @author Kristian Hiim
 */
public interface MasterLockRepository {

    /**
     * Will create a master lock if it does not exists. The lock will be created with the lockTakenTime and
     * lockLastUpdateTime to {@link Instant#EPOCH} so all nodes can try to acquire it.
     *
     * @param lockName
     *         - Name of the lock to be inserted.
     * @param nodeName
     *         - NodeName of the host currently having the lock.
     * @return - true if a new lock was created.
     */
    boolean tryCreateLock(String lockName, String nodeName);

    /**
     * Tries to acquire the master lock. If this manages to update the lock_name it will mean this host has the lock for
     * 5 minutes, during those 5 minutes it should try to run {@link #keepLock(String, String)} so it kan keep it for as
     * long as it needs.
     *
     * @param lockName
     *         - Name of the lock that it should attempt to aquire.
     * @param nodeName
     *         - Host name of the server that should keep the lock.
     * @return - 1 if the lock where aquired. 0 if it did not manage to aquire the lock.
     */
    boolean tryAcquireLock(String lockName, String nodeName);

    /**
     * Allows for the node that currenly has the lock to release it by setting it to {@link Instant#EPOCH}
     *
     * @param lockName
     *         - Lock name to release
     * @param nodeName
     *         - The node name that is the current master node.
     */
    boolean releaseLock(String lockName, String nodeName);

    /**
     * Used for the running host to keep the lock for 5 more minutes. If the <b>lock_last_updated_time</b> is updated
     * that means this host still has this master lock for another 5 minutes. After 5 minutes it means no-one has it
     * until 10 minutes has passed. At that time it is up for grabs again.
     *
     * @param lockName
     *         - name of the master lock
     * @param nodeName
     *         - host name that currently has the lock. Should be this running host.
     * @return - true if any node is updated. false if we did not manage to keep the lock (we lost the master lock).
     */
    boolean keepLock(String lockName, String nodeName);

    /**
     * Helper method to get all the locks in the masterLock table.
     *
     * @return
     */
    List<MasterLock> getLocks() throws SQLException;

    /**
     * Helper method to get a specific the lock in the masterLock table.
     *
     * @return
     */
    Optional<MasterLock> getLock(String lockName);

}
