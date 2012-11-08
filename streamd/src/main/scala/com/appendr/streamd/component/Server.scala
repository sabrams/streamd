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

import com.appendr.streamd.network.netty._
import com.appendr.streamd.cluster._
import com.appendr.streamd.conf._
import com.appendr.streamd.stream._
import com.appendr.streamd.network.services.{HttpServices, TelnetServices}
import com.appendr.streamd.network.DispatchingNetworkHandler

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object Server {
    def apply(config: Configuration): Server = {
        val serverSpec = new ServerSpec(config)
        serverSpec.validate()

        val clusterSpec = new ClusterSpec(config)
        clusterSpec.validate()

        val moduleSpec = new ModuleSpec(config)
        moduleSpec.validate()

        new Server(serverSpec.apply(), moduleSpec, clusterSpec)
    }
}

sealed class Server(
    private val config: BaseConfig[_],
    private val ms: ModuleSpec,
    private val cs: ClusterSpec) {
    private val cluster = Cluster(cs, config.node)
    private val modules = ms.apply()
    private val dispatch = StreamRoutingDispatcher(modules, cluster)
    private val server = NettyObjectServer(DispatchingNetworkHandler(dispatch))
    private val telnet = TelnetServices()
    private val telnetServer = NettyTextServer(telnet)
    private val http = HttpServices()
    private val httpServer = NettyHttpServer(http)

    modules.foreach {
        m => {
            val service = m.service()
            if (service.isDefined) {
                telnet.registerService(service.get)
                http.registerService(service.get)
            }
        }
    }

    def start() {
        dispatch.start()
        server.start(config.spec.port.value)
        cluster.start()
        telnetServer.start(config.spec.cport.value)
        httpServer.start(NodeDecoder(config.node.get).managementPort)
    }

    def stop() {
        cluster.stop()
        dispatch.stop()
        modules.foreach(m => m.close())
        telnetServer.stop()
        httpServer.stop()
        server.stop()
    }

    def getNode: Node = config.node.get
    def getRouter: Router = cluster
    def getTopology: Topology = cluster
}
