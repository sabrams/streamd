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
import org.jboss.netty.channel._
import org.slf4j.LoggerFactory
import org.jboss.netty.handler.codec.http.websocketx._
import java.util.Collections
import org.jboss.netty.handler.codec.http.{HttpResponse, HttpRequestEncoder, HttpResponseDecoder}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object NettyWebSocketHandler {
    def apply(h: WebSocketClientHandshaker) = new NettyWebSocketHandler(h)
}

class NettyWebSocketHandler(private val h: WebSocketClientHandshaker)
    extends SimpleChannelUpstreamHandler {
    private val log = LoggerFactory.getLogger(getClass)

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
        val ch = ctx.getChannel
        val m = e.getMessage

        if (!h.isHandshakeComplete) {
            h.finishHandshake(ch, m.asInstanceOf[HttpResponse])
            if (log.isInfoEnabled) log.info("WebSocket Client connected!")
        }
        else {
            if (m.isInstanceOf[HttpResponse]) throw new RuntimeException("Unexpected response.")
            if (m.isInstanceOf[CloseWebSocketFrame]) ch.close()
        }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
        log.warn("Unexpected exception from downstream.", e.getCause)
        e.getChannel.close
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