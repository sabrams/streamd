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

import org.slf4j.LoggerFactory
import java.util.UUID
import com.appendr.streamd.network.netty._
import com.appendr.streamd.cluster.{Router, Cluster, Topology, Node}
import com.appendr.streamd.conf._
import com.appendr.streamd.stream._
import com.appendr.streamd.stream.StreamEvent
import com.appendr.streamd.stream.Source
import com.appendr.streamd.network.DispatchingNetworkHandler

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object Server {
    def apply(config: Configuration): Server = {
        val serverSpec = new ServerSpec(config)
        serverSpec.validate()

        val clusterSpec = new ClusterSpec(config)
        clusterSpec.validate()

        val pluginSpec = new PluginSpec(config)
        pluginSpec.validate()

        new Server(serverSpec.apply(), pluginSpec, clusterSpec)
    }
}

sealed class Server(
    private val config: BaseConfig[_],
    private val ps: PluginSpec,
    private val cs: ClusterSpec) {
    private val cluster = Cluster(cs, config.node, config.codec.value)
    private val server = NettyServer()
    private val plugin = ps.apply()
    private val dispatch = StreamRoutingDispatcher(plugin.proc, cluster, plugin.store, plugin.sink)

    def start() {
        // TODO: plugins need to be dynamically loaded and unloaded (version 0.1 it is static)
        // TODO: demux streams to multiple plugins by streamId (version 0.1 supports single stream)
        dispatch.start()
        server.start(config.spec.port.value, DispatchingNetworkHandler(dispatch, config.codec.value))
        cluster.start()
    }

    def stop() {
        dispatch.stop()
        cluster.stop()
        server.stop()
        plugin.close()
    }

    def getNode: Node = config.node.get
    def getRouter: Router = cluster
    def getTopology: Topology = cluster
}

object Client {
    def apply(config: Configuration): Server = {
        val clientSpec = new ClientSpec(config)
        clientSpec.validate()

        val clusterSpec = new ClusterSpec(config)
        clusterSpec.validate()

        val pluginSpec = new PluginSpec(config)
        pluginSpec.validate()

        new Server(clientSpec.apply(), pluginSpec, clusterSpec)
    }

    implicit def server2Client(from: Server) = new Client(from)
}

sealed class Client(val server: Server) {
    private val log = LoggerFactory.getLogger(getClass)
    private val streamId = UUID.randomUUID().toString

    def start() { server.start() }
    def stop() { server.stop() }

    def postEvent(s: StreamTuple, mep: Exchange) {
        val event = StreamEvent(Source(streamId, server.getNode, mep), Some(s))
        val cluster = server.getTopology.asInstanceOf[Cluster]
        val route = cluster.getRoute(event)
        if (log.isDebugEnabled) log.debug("--- postEvent: " + event + " to: " + route)
        if (route.isDefined) cluster.route(route.get, event)
    }
}
