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
package com.appendr.streamd.network.netty

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import java.util.concurrent.Executors
import java.net.InetSocketAddress
import org.jboss.netty.channel.group.DefaultChannelGroup
import com.appendr.streamd.component.ServerComponent
import com.appendr.streamd.network.NetworkHandler

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object NettyServer {
    def apply() = new NettyServer
}

class NettyServer extends ServerComponent {
    private val channels = new DefaultChannelGroup("NettyServer")
    private val bootstrap = new ServerBootstrap(
        new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool,
            Executors.newCachedThreadPool))

    def start(port: Int, h: NetworkHandler, opts: Option[Map[String, Any]]) {
        bootstrap.setPipelineFactory(ObjectPipelineFactory(h))
        if (opts.isDefined) opts.get.foreach(kv => bootstrap.setOption(kv._1, kv._2))
        channels.add(bootstrap.bind(new InetSocketAddress(port)))
        channels.setReadable(true)
    }

    def stop() {
        channels.setReadable(false)
        channels.close.awaitUninterruptibly
        bootstrap.getFactory.releaseExternalResources()
    }
}

object NettyTextServer {
    def apply() = new NettyTextServer
}

class NettyTextServer extends NettyServer {
    private val channels = new DefaultChannelGroup("NettyServer")
    private val bootstrap = new ServerBootstrap(
        new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool,
            Executors.newCachedThreadPool))

    override def start(port: Int, h: NetworkHandler, opts: Option[Map[String, Any]]) {
        bootstrap.setPipelineFactory(TextPipelineFactory(h))
        if (opts.isDefined) opts.get.foreach(kv => bootstrap.setOption(kv._1, kv._2))
        channels.add(bootstrap.bind(new InetSocketAddress(port)))
        channels.setReadable(true)
    }
}