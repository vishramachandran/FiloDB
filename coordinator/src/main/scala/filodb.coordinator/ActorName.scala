package filodb.coordinator

import akka.actor.{ActorPath, Address, RootActorPath}

object ActorName {

  val NodeGuardianName = "node"
  val CoordinatorName = "coordinator"

  /* The actor name of the child singleton actor */
  val ClusterSingletonManagerName = "nodecluster"
  val ClusterSingletonName = "singleton"
  val ClusterSingletonProxyName = "singletonProxy"
  val ShardName = "shard-coordinator"

  /** MemstoreCoord Worker name prefix. Naming pattern is prefix-datasetRef.dataset */
  val Ingestion = "ingestion"
  /** Query Worker name prefix. Naming pattern is prefix-datasetRef.dataset */
  val Query = "query"

  /* Actor Paths */
  def nodeCoordinatorPath(addr: Address, v2ClusterEnabled: Boolean): ActorPath = {
    if (v2ClusterEnabled) RootActorPath(addr) / "user" / CoordinatorName
    else RootActorPath(addr) / "user" / NodeGuardianName / CoordinatorName
  }

}
