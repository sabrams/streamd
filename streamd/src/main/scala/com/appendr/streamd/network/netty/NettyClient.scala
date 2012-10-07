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

import java.net.{URI, InetSocketAddress}
import java.util.concurrent.Executors
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel.{ChannelPipeline, ChannelPipelineFactory, Channels, Channel}
import org.slf4j.LoggerFactory
import com.appendr.streamd.component.ClientComponent
import com.appendr.streamd.network.NoOpNetworkHandler
import com.appendr.streamd.stream.codec.Codec
import org.jboss.netty.handler.codec.http.websocketx.{CloseWebSocketFrame, TextWebSocketFrame, WebSocketVersion, WebSocketClientHandshakerFactory}
import java.util.Collections
import org.jboss.netty.handler.codec.http.{HttpRequestEncoder, HttpResponseDecoder}

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

class NettyWebSocketClient(private val uri: URI) {
    private val log = LoggerFactory.getLogger(getClass)
    @volatile private var channel: Channel = null
    private val bootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool,
            Executors.newCachedThreadPool))
    private val hs = new WebSocketClientHandshakerFactory().newHandshaker(
        uri, WebSocketVersion.V13, null, false, Collections.emptyMap())

    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
        def getPipeline: ChannelPipeline = {
            val pipeline = Channels.pipeline()
            pipeline.addLast("decoder", new HttpResponseDecoder())
            pipeline.addLast("encoder", new HttpRequestEncoder())
            pipeline.addLast("ws-handler", new NettyWebSocketHandler(hs))
            pipeline
        }
    })

    def connect() {
        if (channel == null || channel.isConnected == false) {
            if (!uri.getScheme.equals("ws")) throw new IllegalArgumentException("Bad protocol: " + uri.getScheme)
            // Make a new connection.
            val cf = bootstrap.connect(new InetSocketAddress(uri.getHost, uri.getPort))
            cf.syncUninterruptibly()
            channel = cf.getChannel
            hs.handshake(channel).syncUninterruptibly()
        }
    }

    def disconnect() {
        channel.write(new CloseWebSocketFrame())
        channel.getCloseFuture.awaitUninterruptibly()
    }

    def send(msg: String) {
        if (channel.isWritable) {
            if (log.isDebugEnabled) log.debug("Writing to channel: " + msg)
            channel.write(new TextWebSocketFrame(msg))
        }
    }
}