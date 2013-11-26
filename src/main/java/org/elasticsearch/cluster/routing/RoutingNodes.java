/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.shard.ShardId;

import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

/**
 * {@link RoutingNodes} represents a copy the routing information contained in
 * the {@link ClusterState cluster state}.
 */
public class RoutingNodes implements Iterable<RoutingNode> {

    private final MetaData metaData;

    private final ClusterBlocks blocks;

    private final RoutingTable routingTable;

    private final Map<String, RoutingNode> nodesToShards = newHashMap();

    private final List<MutableShardRouting> unassigned = newArrayList();

    private final List<MutableShardRouting> ignoredUnassigned = newArrayList();

    private final RoutingManager manager;
    
    private Set<ShardId> clearPostAllocationFlag;

    private final Map<String, ObjectIntOpenHashMap<String>> nodesPerAttributeNames = new HashMap<String, ObjectIntOpenHashMap<String>>();

    private static RoutingNodes instance;

    public static RoutingNodes getInstance() {
        return instance;
    }

    public static RoutingNodes createInstance( ClusterState state ) {
        instance = new RoutingNodes( state );
        return getInstance();
    }

    public static RoutingManager manager() {
        return getInstance().manager;
    }

    private RoutingNodes(ClusterState clusterState) {
        this.metaData = clusterState.metaData();
        this.blocks = clusterState.blocks();
        this.routingTable = clusterState.routingTable();
        this.manager = new RoutingManager( this );

        Map<String, List<MutableShardRouting>> nodesToShards = newHashMap();
        // fill in the nodeToShards with the "live" nodes
        for (DiscoveryNode node : clusterState.nodes().dataNodes().values()) {
            nodesToShards.put(node.id(), new ArrayList<MutableShardRouting>());
        }

        // fill in the inverse of node -> shards allocated
        for (IndexRoutingTable indexRoutingTable : routingTable.indicesRouting().values()) {
            for (IndexShardRoutingTable indexShard : indexRoutingTable) {
                for (ShardRouting shard : indexShard) {
                    // to get all the shards belonging to an index, including the replicas,
                    // we define a replica set and keep track of it. A replica set is identified
                    // by the ShardId, as this is common for primary and replicas.
                    // A replica Set might have one (and not more) replicas with the state of RELOCATING.
                    if (shard.assignedToNode()) {
                        List<MutableShardRouting> entries = nodesToShards.get(shard.currentNodeId());
                        if (entries == null) {
                            entries = newArrayList();
                            nodesToShards.put(shard.currentNodeId(), entries);
                        }
                        MutableShardRouting sr = new MutableShardRouting(shard);
                        entries.add( sr );
                        manager.addToReplicaSet( sr );
                        if (shard.relocating()) {
                            entries = nodesToShards.get(shard.relocatingNodeId());
                            manager.relocatingReplicaSets.add( shard.shardId() );
                            if (entries == null) {
                                entries = newArrayList();
                                nodesToShards.put(shard.relocatingNodeId(), entries);
                            }
                            // add the counterpart shard with relocatingNodeId reflecting the source from which
                            // it's relocating from.
                            sr = new MutableShardRouting(shard.index(), shard.id(), shard.relocatingNodeId(),
                                    shard.currentNodeId(), shard.primary(), ShardRoutingState.INITIALIZING, shard.version());
                            entries.add( sr );
                            manager.addToReplicaSet( sr );
                        }
                        else if ( !shard.active() ) { // shards that are initializing without being relocated
                            if ( shard.primary() )
                                manager.inactivePrimaryCount++;
                            manager.inactiveShardCount++;
                        }
                    } else {
                        MutableShardRouting sr = new MutableShardRouting(shard);
                        manager.addToReplicaSet( sr );
                        unassigned.add( sr );
                        if ( shard.primary() )
                            manager.unassignedPrimaryCount++;

                    }
                }
            }
        }
        for (Map.Entry<String, List<MutableShardRouting>> entry : nodesToShards.entrySet()) {
            String nodeId = entry.getKey();
            this.nodesToShards.put(nodeId, new RoutingNode(nodeId, clusterState.nodes().get(nodeId), entry.getValue()));
        }
    }

    @Override
    public Iterator<RoutingNode> iterator() {
        return nodesToShards.values().iterator();
    }

    public RoutingTable routingTable() {
        return routingTable;
    }

    public RoutingTable getRoutingTable() {
        return routingTable();
    }

    public MetaData metaData() {
        return this.metaData;
    }

    public MetaData getMetaData() {
        return metaData();
    }

    public ClusterBlocks blocks() {
        return this.blocks;
    }

    public ClusterBlocks getBlocks() {
        return this.blocks;
    }

    public int requiredAverageNumberOfShardsPerNode() {
        int totalNumberOfShards = 0;
        // we need to recompute to take closed shards into account
        for (IndexMetaData indexMetaData : metaData.indices().values()) {
            if (indexMetaData.state() == IndexMetaData.State.OPEN) {
                totalNumberOfShards += indexMetaData.totalNumberOfShards();
            }
        }
        return totalNumberOfShards / nodesToShards.size();
    }

    public boolean hasUnassigned() {
        return !unassigned.isEmpty();
    }

    public List<MutableShardRouting> ignoredUnassigned() {
        return this.ignoredUnassigned;
    }

    public List<MutableShardRouting> unassigned() {
        return this.unassigned;
    }

    public List<MutableShardRouting> getUnassigned() {
        return unassigned();
    }

    public Map<String, RoutingNode> nodesToShards() {
        return nodesToShards;
    }

    public Map<String, RoutingNode> getNodesToShards() {
        return nodesToShards();
    }

    /**
     * Clears the post allocation flag for the provided shard id. NOTE: this should be used cautiously
     * since it will lead to data loss of the primary shard is not allocated, as it will allocate
     * the primary shard on a node and *not* expect it to have an existing valid index there.
     */
    public void addClearPostAllocationFlag(ShardId shardId) {
        if (clearPostAllocationFlag == null) {
            clearPostAllocationFlag = Sets.newHashSet();
        }
        clearPostAllocationFlag.add(shardId);
    }

    public Iterable<ShardId> getShardsToClearPostAllocationFlag() {
        if (clearPostAllocationFlag == null) {
            return ImmutableSet.of();
        }
        return clearPostAllocationFlag;
    }

    public RoutingNode node(String nodeId) {
        return nodesToShards.get(nodeId);
    }

    public ObjectIntOpenHashMap<String> nodesPerAttributesCounts(String attributeName) {
        ObjectIntOpenHashMap<String> nodesPerAttributesCounts = nodesPerAttributeNames.get(attributeName);
        if (nodesPerAttributesCounts != null) {
            return nodesPerAttributesCounts;
        }
        nodesPerAttributesCounts = new ObjectIntOpenHashMap<String>();
        for (RoutingNode routingNode : this) {
            String attrValue = routingNode.node().attributes().get(attributeName);
            nodesPerAttributesCounts.addTo(attrValue, 1);
        }
        nodesPerAttributeNames.put(attributeName, nodesPerAttributesCounts);
        return nodesPerAttributesCounts;
    }

    public boolean hasUnassignedPrimaries() {
        return manager.unassignedPrimaryCount > 0;
    }

    public boolean hasUnassignedShards() {
        return !unassigned.isEmpty();
    }

    public boolean hasInactivePrimaries() {
        return manager.inactivePrimaryCount > 0;
    }

    public boolean hasInactiveShards() {
        return manager.inactiveShardCount > 0;
    }

    public int getRelocatingShardCount() {
        return manager.relocatingReplicaSets.size();
    }

    public MutableShardRouting findPrimaryForReplica(ShardRouting shard) {
        assert !shard.primary();
        List<MutableShardRouting> shards = shardsRoutingFor( shard );
        for (int i = 0; i < shards.size(); i++) {
            MutableShardRouting shardRouting = shards.get(i);
            if ( shardRouting.primary() ) {
                return shardRouting;
            }
        }
        return null;
    }

    public List<MutableShardRouting> shardsRoutingFor(ShardRouting shardRouting) {
        return shardsRoutingFor(shardRouting.index(), shardRouting.id());
    }

    public List<MutableShardRouting> shardsRoutingFor(String index, int shardId) {
        ShardId sid = new ShardId( index, shardId );
        List<MutableShardRouting> shards = manager.replicaSetFor( sid );
        if ( shards == null ) {
            shards = newArrayList();
        }
        // no need to check unassigned array, since the ShardRoutings are in the replica set.
        return shards;
    }

    public int numberOfShardsOfType(ShardRoutingState state) {
        int count = 0;
        for (RoutingNode routingNode : this) {
            count += routingNode.numberOfShardsWithState(state);
        }
        return count;
    }

    public List<MutableShardRouting> shards(Predicate<MutableShardRouting> predicate) {
        List<MutableShardRouting> shards = newArrayList();
        for (RoutingNode routingNode : this) {
            List<MutableShardRouting> nodeShards = routingNode.shards();
            for (int i = 0; i < nodeShards.size(); i++) {
                MutableShardRouting shardRouting = nodeShards.get(i);
                if (predicate.apply(shardRouting)) {
                    shards.add(shardRouting);
                }
            }
        }
        return shards;
    }

    public List<MutableShardRouting> shardsWithState(ShardRoutingState... state) {
        List<MutableShardRouting> shards = newArrayList();
        for (RoutingNode routingNode : this) {
            shards.addAll(routingNode.shardsWithState(state));
        }
        return shards;
    }

    public List<MutableShardRouting> shardsWithState(String index, ShardRoutingState... state) {
        List<MutableShardRouting> shards = newArrayList();
        for (RoutingNode routingNode : this) {
            shards.addAll(routingNode.shardsWithState(index, state));
        }
        return shards;
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder("routing_nodes:\n");
        for (RoutingNode routingNode : this) {
            sb.append(routingNode.prettyPrint());
        }
        sb.append("---- unassigned\n");
        for (MutableShardRouting shardEntry : unassigned) {
            sb.append("--------").append(shardEntry.shortSummary()).append('\n');
        }
        return sb.toString();
    }

}
