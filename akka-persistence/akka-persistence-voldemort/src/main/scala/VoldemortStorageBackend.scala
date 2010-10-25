/**
 *  Copyright (C) 2009-2010 Scalable Solutions AB <http://scalablesolutions.se>
 */

package se.scalablesolutions.akka.persistence.voldemort

import se.scalablesolutions.akka.persistence.common._
import se.scalablesolutions.akka.config.Config.config

import voldemort.client._
import java.lang.String
import voldemort.versioning.Versioned
import collection.JavaConversions
import collection.Map
import java.util.{Properties, Map => JMap}
import collection.immutable._
import voldemort.client.protocol.admin.{AdminClientConfig, AdminClient}

/*
  RequiredReads + RequiredWrites should be > ReplicationFactor for all Voldemort Stores
  In this case all VoldemortBackend operations can be retried until successful, and data should remain consistent
 */

private[akka] object VoldemortStorageBackend extends KVStorageBackend {

  import KVAccess._
  import VoldemortAccess._

  val bootstrapUrlsProp = "bootstrap_urls"
  val clientConfig = config.getConfigMap("akka.storage.voldemort.client") match {
    case Some(configMap) => getClientConfig(configMap.asMap)
    case None => getClientConfig(new HashMap[String, String] + (bootstrapUrlsProp -> "tcp://localhost:6666"))
  }
  val refStore = config.getString("akka.storage.voldemort.store.ref", "Refs")
  val mapStore = config.getString("akka.storage.voldemort.store.map", "Maps")
  val vectorStore = config.getString("akka.storage.voldemort.store.vector", "Vectors")
  val queueStore = config.getString("akka.storage.voldemort.store.queue", "Queues")


  var storeClientFactory: StoreClientFactory = null
  var refs: KVAccess = null
  var maps: KVAccess = null
  var vectors: KVAccess = null
  var queues: KVAccess = null
  resetAccess

  def refAccess = refs

  def mapAccess = maps

  def vectorAccess = vectors

  def queueAccess = queues


  object VoldemortAccess {
    var admin: AdminClient = null
  }

  class VoldemortAccess(val store: String) extends KVAccess {

    import VoldemortAccess._

    val client: StoreClient[Array[Byte], Array[Byte]] = VoldemortStorageBackend.storeClientFactory.getStoreClient(store)

    def put(key: Array[Byte], value: Array[Byte]) = {
      client.put(key, value)
    }

    def getValue(key: Array[Byte]): Array[Byte] = {
      client.getValue(key)
    }

    def getValue(key: Array[Byte], default: Array[Byte]): Array[Byte] = {
      client.getValue(key, default)
    }

    def getAll(keys: Iterable[Array[Byte]]): Map[Array[Byte], Array[Byte]] = {
      JavaConversions.asMap(client.getAll(JavaConversions.asIterable(keys))).map{
        kv =>
          kv match {
            case (key: Array[Byte], versioned: Versioned[Array[Byte]]) => (key -> versioned.getValue)
          }
      }
    }

    def delete(key: Array[Byte]) = {
      client.delete(key)
    }

    def drop() = {
      admin.truncate(0, store)
    }
  }


  def getClientConfig(configMap: Map[String, String]): Properties = {
    val properites = new Properties
    configMap.foreach{
      keyval => keyval match {
        case (key, value) => properites.setProperty(key.asInstanceOf[java.lang.String], value.asInstanceOf[java.lang.String])
      }
    }
    properites
  }

  def initStoreClientFactory(): StoreClientFactory = {
    if (storeClientFactory ne null) {
      storeClientFactory.close
    }

    if (clientConfig.getProperty(bootstrapUrlsProp, "none").startsWith("tcp")) {
      new SocketStoreClientFactory(new ClientConfig(clientConfig))
    } else if (clientConfig.getProperty(bootstrapUrlsProp, "none").startsWith("http")) {
      new HttpStoreClientFactory(new ClientConfig(clientConfig))
    } else {
      throw new IllegalArgumentException("Unknown boostrapUrl syntax" + clientConfig.getProperty(bootstrapUrlsProp, "No Bootstrap URLs defined"))
    }
  }

  def initAdminClient(): AdminClient = {
    if (VoldemortAccess.admin ne null) {
       VoldemortAccess.admin.stop
    }

    new AdminClient(VoldemortStorageBackend.clientConfig.getProperty(VoldemortStorageBackend.bootstrapUrlsProp), new AdminClientConfig)

  }

  def initKVAccess = {
    refs = new VoldemortAccess(refStore)
    maps = new VoldemortAccess(mapStore)
    vectors = new VoldemortAccess(vectorStore)
    queues = new VoldemortAccess(queueStore)
  }

  def resetAccess() {
    storeClientFactory = initStoreClientFactory
    VoldemortAccess.admin = initAdminClient
    initKVAccess
  }


}