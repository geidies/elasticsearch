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

import org.elasticsearch.index.shard.ShardId;

import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

/**
 *  Manager class to mutate ShardRoutings, RoutingNodes and keep statistics
 *  that the AllocationDecider classes can use without having to loop over
 *  all shards assigned to a node.
 */
public class RoutingManager {

    private RoutingNodes parent;
    /**
     * package-private access. Really, only RoutingNodes should instantiate this.
     */
    RoutingManager( RoutingNodes parent ) {
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
    public void assignShardToNode( MutableShardRouting shard, String nodeId ) {

        // state will not change if the shard is already initializing.
        ShardRoutingState oldState = shard.state();
        
        shard.assignToNode( nodeId );

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
    public void relocateShard( MutableShardRouting shard, String nodeId ) {
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
    public void cancelRelocationForShard( MutableShardRouting shard ) {
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
    public void deassignShard( MutableShardRouting shard ) {
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
    public void startedShard( MutableShardRouting shard ) {
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
    public List<MutableShardRouting> replicaSetFor( MutableShardRouting shard ) {
        return replicaSetFor( shard.shardId() );
    }

    /**
     * Return a list of shards belonging to a replica set
     * 
     * @param shardId the {@link ShardId} for which to retrieve the replica set
     * @return an unmodifiable List of the replica set
     */
    public List<MutableShardRouting> replicaSetFor( ShardId shardId ) {
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
    void addToReplicaSet( MutableShardRouting shard ) {
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
    void markRelocating( MutableShardRouting shard ) {
        relocatingReplicaSets.add( shard.shardId() );
    }
}
