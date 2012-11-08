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
import com.appendr.streamd.network.{ControlMessage, NetworkHandler}
import org.jboss.netty.channel._
import org.slf4j.LoggerFactory
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.handler.codec.http.HttpVersion._
import org.jboss.netty.buffer.ChannelBuffers
import com.appendr.streamd.network.NetworkMessage

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object NettyObjectServer {
    def apply(h: NetworkHandler) = new NettyServer(ObjectPipelineFactory(h))
}

object NettyTextServer {
    def apply(h: NetworkHandler) = new NettyServer(TextPipelineFactory(h))
}

object NettyHttpServer {
    def apply(h: NetworkHandler) = new NettyServer(HttpPipelineFactory(h))
}

class NettyServer(
    ppln: ChannelPipelineFactory,
    opts: Option[Map[String, Any]] = None)
    extends ServerComponent {
    protected val channels = new DefaultChannelGroup(ppln.getClass.getName)
    protected val bootstrap = new ServerBootstrap(
        new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool,
            Executors.newCachedThreadPool))

    def start(port: Int) {
        bootstrap.setPipelineFactory(ppln)
        if (opts.isDefined) opts.get.foreach(kv => bootstrap.setOption(kv._1, kv._2))
        channels.add(bootstrap.bind(new InetSocketAddress(port)))
        channels.setReadable(true)
    }

    def stop() {
        channels.setReadable(false)
        channels.close
        bootstrap.getFactory.releaseExternalResources()
    }
}

class NettyHandler(handler: NetworkHandler) extends SimpleChannelUpstreamHandler {
    private val log = LoggerFactory.getLogger(getClass)

    override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
        if (e.isInstanceOf[ChannelStateEvent] && log.isInfoEnabled) log.info(e.toString)
        super.handleUpstream(ctx, e)
    }

    override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
        super.channelConnected(ctx, e)
        handler.handleConnect(e.getChannel.getRemoteAddress)
    }

    override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
        super.channelDisconnected(ctx, e)
        handler.handleDisconnect(e.getChannel.getRemoteAddress)
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
        respond(handler.handleMessage(e.getMessage), e)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
        log.warn("Unexpected exception from downstream.", e.getCause)
        e.getChannel.close
    }

    protected def respond(m: Option[NetworkMessage], e: MessageEvent) {
        if (m.isDefined) {
            val msg: NetworkMessage = m.get
            val future = e.getChannel.write(msg.msg)
            if (msg.ctrlMsg.equals(ControlMessage.CLOSE))
                future.addListener(ChannelFutureListener.CLOSE)
        }
    }
}

class NettyEventHandler(handler: NetworkHandler) extends NettyHandler(handler) {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
        handler.handleMessage(e)
    }
}

abstract class NettyHttpNetworkHandler extends NetworkHandler {
    def handleMessage(msg: Object): Option[NetworkMessage] = {
        val request = msg.asInstanceOf[MessageEvent].getMessage.asInstanceOf[HttpRequest]
        val decoder = new QueryStringDecoder(request.getUri)
        val path = decoder.getPath
        val parts = path.split("/").filter(p => !p.isEmpty)
        val event = msg.asInstanceOf[MessageEvent]

        // only support get and post
        request.getMethod match {
            case HttpMethod.GET => handleGet(parts, event)
            case HttpMethod.POST => handlePost(parts, event)
            case _ => do405(event)
        }

        None
    }

    def handleRequest(cmd: String, path: Array[String]): String
    def handleRequest(cmd: String): Boolean

    private def handleGet(path: Array[String], e: MessageEvent) {
        path.length match {
            case 0 => do404(e)
            case 1 => do404(e)
            case _ => {
                val key = path(0) + "/" + path(1)
                if (!handleRequest(key)) do404(e)
                else writeResponse(handleRequest(key, path.tail ++ getContent(e)), e)
            }
        }
    }

    private def handlePost(path: Array[String], e: MessageEvent) {
        handleGet(path, e)
    }

    private def getContent(e: MessageEvent) = {
        val c = e.getMessage.asInstanceOf[HttpRequest].getContent
        if (c.readable()) Array[String](c.toString(CharsetUtil.UTF_8))
        else Array[String]()
    }

    private def do405(e: MessageEvent) {
        e.getChannel.write(new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.METHOD_NOT_ALLOWED))
        e.getChannel.close()
    }

    private def do404(e: MessageEvent) {
        e.getChannel.write(new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.NOT_FOUND))
        e.getChannel.close()
    }

    private def writeResponse(s: String, e: MessageEvent) {
        val response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.OK)
        response.setContent(ChannelBuffers.copiedBuffer(s, CharsetUtil.UTF_8))
        response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8")
        e.getChannel.write(response).addListener(ChannelFutureListener.CLOSE)
    }
}

class NettyNoOpHandler extends SimpleChannelUpstreamHandler {
    private val log = LoggerFactory.getLogger(getClass)

    override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
        if (e.isInstanceOf[ChannelStateEvent]) log.info(e.toString)
        super.handleUpstream(ctx, e)
    }

    override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
        super.channelConnected(ctx, e)
    }

    override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
        super.channelDisconnected(ctx, e)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
        log.warn("Unexpected exception from downstream.", e.getCause)
        e.getChannel.close
    }
}