/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.replication.storage;

import static org.apache.asterix.common.replication.IPartitionReplica.PartitionReplicaStatus.CATCHING_UP;
import static org.apache.asterix.common.replication.IPartitionReplica.PartitionReplicaStatus.DISCONNECTED;
import static org.apache.asterix.common.replication.IPartitionReplica.PartitionReplicaStatus.IN_SYNC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ReplicationException;
import org.apache.asterix.common.replication.IPartitionReplica;
import org.apache.asterix.common.storage.ReplicaIdentifier;
import org.apache.asterix.replication.functions.ReplicationProtocol;
import org.apache.asterix.replication.recovery.ReplicaSynchronizer;
import org.apache.hyracks.util.JSONUtil;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.annotations.ThreadSafe;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@ThreadSafe
public class PartitionReplica implements IPartitionReplica {

    private static final Logger LOGGER = Logger.getLogger(PartitionReplica.class.getName());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int INITIAL_BUFFER_SIZE = StorageUtil.getIntSizeInBytes(4, StorageUtil.StorageUnit.KILOBYTE);
    private final INcApplicationContext appCtx;
    private final ReplicaIdentifier id;
    private ByteBuffer reusbaleBuf;
    private PartitionReplicaStatus status = DISCONNECTED;
    private SocketChannel sc;

    public PartitionReplica(ReplicaIdentifier id, INcApplicationContext appCtx) {
        this.id = id;
        this.appCtx = appCtx;
    }

    @Override
    public synchronized PartitionReplicaStatus getStatus() {
        return status;
    }

    @Override
    public ReplicaIdentifier getIdentifier() {
        return id;
    }

    public synchronized void sync() {
        if (status == IN_SYNC || status == CATCHING_UP) {
            return;
        }
        setStatus(CATCHING_UP);
        appCtx.getThreadExecutor().execute(() -> {
            try {
                new ReplicaSynchronizer(appCtx, this).sync();
                setStatus(IN_SYNC);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e, () -> "Failed to sync replica " + this);
                setStatus(DISCONNECTED);
            } finally {
                close();
            }
        });
    }

    public synchronized SocketChannel getChannel() {
        try {
            if (sc == null || !sc.isOpen() || !sc.isConnected()) {
                sc = SocketChannel.open();
                sc.configureBlocking(true);
                sc.connect(id.getLocation());
            }
            return sc;
        } catch (IOException e) {
            throw new ReplicationException(e);
        }
    }

    public synchronized void close() {
        try {
            if (sc != null && sc.isOpen()) {
                ReplicationProtocol.sendGoodbye(sc);
                sc.close();
                sc = null;
            }
        } catch (IOException e) {
            throw new ReplicationException(e);
        }
    }

    public synchronized ByteBuffer gerReusableBuffer() {
        if (reusbaleBuf == null) {
            reusbaleBuf = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);
        }
        return reusbaleBuf;
    }

    private JsonNode asJson() {
        ObjectNode json = OBJECT_MAPPER.createObjectNode();
        json.put("id", id.toString());
        json.put("state", status.name());
        return json;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionReplica that = (PartitionReplica) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        try {
            return JSONUtil.convertNode(asJson());
        } catch (JsonProcessingException e) {
            throw new ReplicationException(e);
        }
    }

    private synchronized void setStatus(PartitionReplicaStatus status) {
        LOGGER.info(() -> "Replica " + this + " status changing: " + this.status + " -> " + status);
        this.status = status;
    }
}