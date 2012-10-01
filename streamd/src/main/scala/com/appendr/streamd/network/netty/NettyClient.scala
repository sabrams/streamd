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

import java.net.InetSocketAddress
import java.util.concurrent.Executors
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel.Channel
import org.slf4j.LoggerFactory
import com.appendr.streamd.component.ClientComponent
import com.appendr.streamd.network.NoOpNetworkHandler
import com.appendr.streamd.stream.codec.Codec

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object NettyClient {
    def apply[T](name: String, codec: Codec[T]) = {
        new NettyClient(name, codec)
    }
}

class NettyClient[T](val name: String, private val codec: Codec[T])
    extends ClientComponent[T] {
    private val log = LoggerFactory.getLogger(getClass)
    @volatile private var channel: Channel = null
    private val bootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool,
            Executors.newCachedThreadPool))

    bootstrap.setPipelineFactory(ByteArrayPipelineFactory(new NoOpNetworkHandler))

    def connect(address: InetSocketAddress) {
        if (channel == null || channel.isConnected == false) {
            // Make a new connection.
            val connectFuture = bootstrap.connect(address)
            channel = connectFuture.awaitUninterruptibly.getChannel
        }
    }

    def disconnect() {
        channel.disconnect.awaitUninterruptibly
    }

    def send(t: T) {
        if (channel.isWritable) {
            if (log.isDebugEnabled) log.debug("Writing to channel: " + t)
            channel.write(codec.encode(t))
        }
    }
}