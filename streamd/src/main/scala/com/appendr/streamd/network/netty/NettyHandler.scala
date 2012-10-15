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

import org.slf4j.LoggerFactory
import org.jboss.netty.channel._
import com.appendr.streamd.network.{NetworkMessage, ControlMessage, NetworkHandler}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

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
        val result = handler.handleMessage(e.getMessage)
        if (result.isDefined) {
            val msg: NetworkMessage = result.get
            val future = e.getChannel.write(msg.msg)
            if (msg.ctrlMsg.equals(ControlMessage.CLOSE))
                future.addListener(ChannelFutureListener.CLOSE)
        }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
        log.warn("Unexpected exception from downstream.", e.getCause)
        e.getChannel.close
    }
}
