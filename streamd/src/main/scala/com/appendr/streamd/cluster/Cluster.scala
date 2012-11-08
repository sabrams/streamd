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
package com.appendr.streamd.cluster

import scala.collection.mutable
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import com.appendr.streamd.cluster.zk.{ZKConfigSpec, ZKClient}
import com.appendr.streamd.network.netty.NettyClient
import com.appendr.streamd.component.{ClientComponent, ClientComponentRegistry}
import com.appendr.streamd.stream.{StreamTuple, StreamEvent}
import com.appendr.streamd.util.{ListableCounterMBean, JMX}
import java.util.concurrent.atomic.AtomicLong

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object Cluster {
    def apply(z: ZKConfigSpec, n: Option[Node]) = {
        new Cluster(z, n)
    }
}

class Cluster(zks: ZKConfigSpec, val node: Option[Node])
    extends Topology with Router with ListableCounterMBean {
    private val log = LoggerFactory.getLogger(getClass)
    private val count = new AtomicLong(0L)
    private val lastCount = new AtomicLong(0L)
    val ccr = ClientComponentRegistry[StreamEvent]()
    val map = new mutable.HashMap[String, String]
    var zk: ZKClient = null

    def start() {
        // start it up
        zk = zks.apply()

        try { zk.createBase }
        catch { case e: Exception => log.info(e.getMessage) }

        // self register this node
        if (node.isDefined) {
            val n = node.get
            zk.createEphemeral(n.name, n.data.getBytes)
        }

        // build the local topology
        val children = zk.getChildren.toList
        val nodes = children.map((s: String) => new Node(s, new String(zk.get(s))))
        nodes.map((n: Node) => if (!isThisNode(n)) addClient(n))
        zk.watchAllChildren(map, (ba: Array[Byte]) => new String(ba), (s: String) => update(s))

        JMX.register(this, getName())
    }

    def stop() {
        map.synchronized {
            if (node.isDefined) zk.delete(node.get.name)
            val clients = ccr.getClientNames.map((s: String) => ccr.removeClient(s))
            clients.map(_.get.disconnect())
        }

        try {
            zk.close()
            JMX.unregister(this, getName())
        }
        catch {
            case e: Exception => log.warn(e.getMessage)
        }
    }

    def getRoute(e: StreamEvent): Option[Node] = {
        e.stream match {
            case Some(t: StreamTuple) => getRoute(t)
            case None => None
        }
    }

    def getRoute(t: StreamTuple): Option[Node] = {
        t._3.get(t._2) match {
            case Some(s: String) => Some(getNode(s))
            case None => None
        }
    }

    def route(n: Node, e: StreamEvent) {
        val cc = ccr.getClient(n.name)
        cc match {
            case Some(cc: ClientComponent[StreamEvent]) => {
                cc.send(e)
                count.incrementAndGet()
                lastCount.set(System.currentTimeMillis())
            }
            case None => {
                if (isThisNode(n)) log.warn("--- Route to self ignored: " + n)
                else log.warn("--- No client for node: " + n)
            }
        }
    }

    def isThisNode(n: Node): Boolean = {
        var res = false
        if (node.isDefined) res = n.equals(node)
        res
    }

    def getName() = "Cluster-" + this.hashCode()
    def getCount() = count.longValue()
    def getTime() = lastCount.longValue()
    def getList(): Array[String] = getNodes.map(n => n.name + " " + n.data).toArray

    private def update(s: String) {
        map.synchronized {
            map.get(s) match {
                case None => this -= removeClient(s)
                case Some(x) => {
                    val n = addClient(new Node(s, x))
                    if (NodeDecoder(n).isRoutable) this += n
                }
            }
        }
    }

    private def addClient(n: Node): Node = {
        if (!n.equals(node) && !ccr.exists(n.name)) {
            val nd = NodeDecoder.apply(n)
            val client: ClientComponent[StreamEvent] = NettyClient[StreamEvent](nd.name)
            client.connect(new InetSocketAddress(nd.hpe._1, nd.hpe._2.toInt))
            ccr.addClient(nd.name, client)
        }

        n
    }

    private def removeClient(s: String): String = {
        val cc = ccr.removeClient(s)
        cc match {
            case Some(cc: ClientComponent[StreamEvent]) => cc.disconnect()
            case _=>
        }

        s
    }
}