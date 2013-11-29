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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import gnu.trove.map.hash.TObjectIntHashMap;
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

    private final Map<String, TObjectIntHashMap<String>> nodesPerAttributeNames = new HashMap<String, TObjectIntHashMap<String>>();

    /**
     * The {@link RoutingNode} managed by this need access to the {@link RoutingManager}.
     * As they are not aware of the RoutingNodes, they need a static path to access it.
     */
    private static RoutingNodes instance;

    public RoutingNodes(ClusterState clusterState) {
        this.metaData     = clusterState.metaData();
        this.blocks       = clusterState.blocks();
        this.routingTable = clusterState.routingTable();
        this.instance     = this;
        this.manager      = new RoutingManager( this );

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

    public TObjectIntHashMap<String> nodesPerAttributesCounts(String attributeName) {
        TObjectIntHashMap<String> nodesPerAttributesCounts = nodesPerAttributeNames.get(attributeName);
        if (nodesPerAttributesCounts != null) {
            return nodesPerAttributesCounts;
        }
        nodesPerAttributesCounts = new TObjectIntHashMap<String>();
        for (RoutingNode routingNode : this) {
            String attrValue = routingNode.node().attributes().get(attributeName);
            nodesPerAttributesCounts.adjustOrPutValue(attrValue, 1, 1);
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

    /**
     * @see {@link RoutingManager#assignShardToNode(MutableShardRouting,String)}
     */
    public void assignShardToNode( MutableShardRouting shard, String nodeId ) {
        manager.assignShardToNode( shard, nodeId );
    }

    /**
     * @see {@link RoutingManager#relocateShard(MutableShardRouting,String)}
     */
    public void relocateShard( MutableShardRouting shard, String nodeId ) {
        manager.relocateShard( shard, nodeId );
    }

    /**
     * @see {@link RoutingManager#cancelRelocationForShard(MutableShardRouting)}
     */
    public void cancelRelocationForShard( MutableShardRouting shard ) {
        manager.cancelRelocationForShard( shard );
    }

    /**
     * @see {@link RoutingManager#deassignShard(MutableShardRouting)}
     */
    public void deassignShard( MutableShardRouting shard ) {
        manager.deassignShard( shard );
    }

    /**
     * @see {@link RoutingManager#startedShard(MutableShardRouting)}
     */
    public void startedShard( MutableShardRouting shard ) {
        manager.startedShard( shard );
    }
    /**
     * @see {@link RoutingManager#replicaSetFor(MutableShardRouting)}
     */
    public List<MutableShardRouting> replicaSetFor( MutableShardRouting shard ) {
        return manager.replicaSetFor( shard );
    }

    /**
     * @see {@link RoutingManager#replicaSetFor(ShardId)}
     */
    public List<MutableShardRouting> replicaSetFor( ShardId shardId ) {
        return manager.replicaSetFor( shardId );
    }

    /**
     * The {@link RoutingManager} instance that is used exclusively to 
     * update ShardRoutings. This does book-keeping on the operations performed
     * when transforming cluster state through e.g. assigning and relocating 
     * shards; these numbers can be retrieved through helper methods
     * {@link #hasUnassignedPrimaries()}
     * {@link #hasUnassignedShards()}
     * {@link #hasInactivePrimaries()}
     * {@link #hasInactiveShards()}
     * {@link #getRelocatingShardCount()}
     *
     * It also keeps track of the replica sets in the cluster, that is the primary
     * and all replicas (if any), including initializing and relocating in case 
     * relocation happens. This speeds up {@link #shardsRoutingFor(ShardRouting)} and
     * {@link #findPrimaryForReplica(ShardRouting)}, which previously had to loop over
     * all shards,
     *
     * @return the manager instance used to manage the ShardRoutings in these RoutingNodes
     */
    private class RoutingManager {

        private RoutingNodes parent;
        
        protected RoutingManager( RoutingNodes parent ) {
            this.parent = parent;
        } 

        private final Map<ShardId, List<MutableShardRouting>> replicaSets = newHashMap();

        /*
         * all package-private
         */
        int unassignedPrimaryCount = 0;

        int inactivePrimaryCount = 0;

        int inactiveShardCount   = 0;

        Set<ShardId> relocatingReplicaSets = new HashSet<ShardId>();


        /**
         * Assign a shard to a node. This will increment the inactiveShardCount counter
         * and the inactivePrimaryCount counter if the shard is the primary.
         * In case the shard is already assigned and started, it will be marked as 
         * relocating, which is accounted for, too, so the number of concurrent relocations
         * can be retrieved easily.
         * This method can be called several times for the same shard, only the first time
         * will change the state.
         *
         * INITIALIZING => INITIALIZING
         * UNASSIGNED   => INITIALIZING
         * STARTED      => RELOCATING
         * RELOCATING   => RELOCATING
         *
         * @param shard the shard to be assigned
         * @param nodeId the nodeId this shard should initialize on or relocate from
         */
        protected void assignShardToNode( MutableShardRouting shard, String nodeId ) {

            // state will not change if the shard is already initializing.
            ShardRoutingState oldState = shard.state();
            
            shard.assignToNode( nodeId );
            parent.node( nodeId ).add( shard );

            if ( oldState == ShardRoutingState.UNASSIGNED ) {
                inactiveShardCount++;
                if ( shard.primary() ) {
                    unassignedPrimaryCount--;
                    inactivePrimaryCount++;
                }
            }
            if ( shard.state() == ShardRoutingState.RELOCATING ) {
                // this a HashSet. double add no worry.
                relocatingReplicaSets.add( shard.shardId() ); 
            }
            // possibly double/triple adding it to a replica set doesn't matter
            // but make sure we know about the shard.
            addToReplicaSet( shard );
        }
        /**
         * Relocate a shard to another node.
         *
         * STARTED => RELOCATING
         *
         * @param shard the shard to relocate
         * @param nodeId the node to relocate to
         */
        protected void relocateShard( MutableShardRouting shard, String nodeId ) {
            relocatingReplicaSets.add( shard.shardId() );
            shard.relocate( nodeId );
        }


        /**
         * Cancels the relocation of a shard.
         *
         * RELOCATING => STARTED
         *
         * @param shard the shard that was relocating previously and now should be started again.
         */
        protected void cancelRelocationForShard( MutableShardRouting shard ) {
            relocatingReplicaSets.remove( shard.shardId() );
            shard.cancelRelocation();
        }

        /**
         * Unassigns shard from a node.
         * Both relocating and started shards that are deallocated need a new 
         * primary elected.
         *
         * RELOCATING   => null
         * STARTED      => null
         * INITIALIZING => null
         *
         * @param shard the shard to be unassigned.
         */
        protected void deassignShard( MutableShardRouting shard ) {
            if ( shard.state() == ShardRoutingState.RELOCATING ) {
                cancelRelocationForShard( shard );
            }
            if ( shard.primary() )
                unassignedPrimaryCount++;
            shard.deassignNode();
        }

        /**
         * Mark a shard as started.
         * Decreases the counters and marks a replication complete or failed,
         * which is the same for accounting in this class.
         *
         * INITIALIZING => STARTED
         * RELOCATIng   => STARTED
         *
         * @param shard the shard to be marked as started
         */
        protected void startedShard( MutableShardRouting shard ) {
            if ( shard.state() == ShardRoutingState.INITIALIZING 
                 && shard.relocatingNodeId() != null ) {
                relocatingReplicaSets.remove( shard.shardId() );
            }
            inactiveShardCount--;
            if ( shard.primary() ) {
                inactivePrimaryCount--;
            }
            shard.moveToStarted();
        }


        /**
         * Return a list of shards belonging to a replica set
         * 
         * @param shard the shard for which to retrieve the replica set
         * @return an unmodifiable List of the replica set
         */
        protected List<MutableShardRouting> replicaSetFor( MutableShardRouting shard ) {
            return replicaSetFor( shard.shardId() );
        }

        /**
         * Return a list of shards belonging to a replica set
         * 
         * @param shardId the {@link ShardId} for which to retrieve the replica set
         * @return an unmodifiable List of the replica set
         */
        protected List<MutableShardRouting> replicaSetFor( ShardId shardId ) {
            List<MutableShardRouting> replicaSet = replicaSets.get( shardId );
            if ( replicaSet == null ) {
                return null;
            }
            return Collections.unmodifiableList( replicaSet );
        }

        /**
         * Let this class know about a shard, which it then sorts into 
         * its replica set. Package private as only {@link RoutingNodes} 
         * should notify this class of shards during initialization.
         *
         * @param shard the shard to be sorted into its replica set
         */
        protected void addToReplicaSet( MutableShardRouting shard ) {
            List<MutableShardRouting> replicaSet = replicaSets.get( shard.shardId() );
            if ( replicaSet == null ) {
                replicaSet = new ArrayList<MutableShardRouting>();
                replicaSets.put( shard.shardId(), replicaSet );
            }
            replicaSet.add( shard );
        }

        /**
         * marks a replica set as relocating. package private as only 
         * {@link RoutingNodes} should call it during initialization.
         *
         * @param shard a member of the relocating replica set
         */
        protected void markRelocating( MutableShardRouting shard ) {
            relocatingReplicaSets.add( shard.shardId() );
        }
    }

}
