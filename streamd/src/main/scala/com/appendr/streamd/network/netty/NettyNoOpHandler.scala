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

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

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