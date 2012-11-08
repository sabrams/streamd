/**
 * Copyright (C) 2011 apendr.com
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 *   _  _  _  _  _  _| _
 *  (_||_)|_)(/_| |(_||
 *     |  |
 */
package com.appendr.streamd.component

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import collection.mutable
import com.appendr.streamd.util.threading.CachedThreadPoolStrategy
import scalaz._
import Scalaz._

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

trait ServerComponent {
    def start(port: Int)
    def stop()
}

trait ClientComponent[T] {
    def name: String
    def connect(address: InetSocketAddress)
    def disconnect()
    def send(t: T)
}

class ClientComponentAdaptor[T](private val wrapped: ClientComponent[T])
    extends ClientComponent[T] {
    private val a = actor[T]((t: T) => {
        wrapped.send(t)
        System.out.println("- Executing on thread: " + Thread.currentThread().getId)
    })(CachedThreadPoolStrategy.strategy)
    def name = wrapped.name
    def connect(address: InetSocketAddress) { wrapped.connect(address) }
    def disconnect() { wrapped.disconnect() }
    def send(t: T) { actor(a) ! t }
}

// TODO: Check if client is connected
class ClientComponentPool[T](private val clients: List[ClientComponent[T]])
    extends ClientComponent[T] {
    def name = clients(0).name
    def connect(address: InetSocketAddress) { clients.foreach(c => c.connect(address)) }
    def disconnect() { clients.foreach(c => c.disconnect()) }
    def send(t: T) { clients(math.abs(t.hashCode() % clients.length)).send(t) }
}

object ClientComponentRegistry {
    def apply[T](): ClientComponentRegistry[T] = {
        new ClientComponentRegistry
    }
}

class ClientComponentRegistry[T] {
    import scala.collection.JavaConversions._
    private val map: mutable.ConcurrentMap[String, ClientComponent[T]] =
        new ConcurrentHashMap[String, ClientComponent[T]]

    def addClients(clients: List[ClientComponent[T]]) {
        clients.map(c => addClient(c.name, c))
    }

    def addClient(name: String, client: ClientComponent[T]) {
        if (map.containsKey(name))
            throw new RuntimeException(String.format("Duplicate client %s.", name))

        map.put(name, client)
    }

    def removeClient(name: String): Option[ClientComponent[T]] = map.remove(name)
    def getClient(name: String): Option[ClientComponent[T]] = map.get(name)
    def getClientNames: List[String] = map.keySet.toList
    def exists(name: String): Boolean = map.contains(name)
}
