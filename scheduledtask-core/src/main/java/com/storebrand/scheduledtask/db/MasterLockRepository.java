/*
 * Copyright 2022 Storebrand ASA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
     * Node name used to create a lock for a non-existing node. This is used to recreate a missing lock, when we don't
     * know who had the lock before.
     */
    String NON_EXISTING_NODE = "NON-EXISTING-NODE";

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
     * Will try to create a missing lock. The lock will be created for a node named {@link #NON_EXISTING_NODE}, so no
     * one can take the lock before 10 minutes has passed. This is to ensure whoever had the lock before it disappeared
     * have enough time to realize that they don't have the lock.
     *
     * @param lockName
     *         - Name of the lock to be inserted.
     * @return - true if a new lock was created.
     */
    boolean tryCreateMissingLock(String lockName);

    /**
     * Tries to acquire the master lock. If this manages to update the lock_name it will mean this host has the lock for
     * 5 minutes, during those 5 minutes it should try to run {@link #keepLock(String, String)} so it kan keep it for as
     * long as it needs.
     *
     * @param lockName
     *         - Name of the lock that it should attempt to acquire.
     * @param nodeName
     *         - Host name of the server that should keep the lock.
     * @return - true if the lock where acquired. false if it did not manage to acquire the lock.
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
