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

import com.appendr.streamd.network.NetworkHandler
import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.frame.{LengthFieldPrepender, LengthFieldBasedFrameDecoder}
import org.jboss.netty.handler.codec.oneone.{OneToOneEncoder, OneToOneDecoder}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object ByteArrayPipelineFactory {
    def apply(h: NetworkHandler) = {
        new ByteArrayPipelineFactory(h)
    }
}

class ByteArrayPipelineFactory(private val h: NetworkHandler)
    extends ChannelPipelineFactory {
    def getPipeline: ChannelPipeline = {
        Channels.pipeline(
            new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4),
            new ByteArrayDecoder,
            new LengthFieldPrepender(4),
            new ByteArrayEncoder,
            new NettyHandler(h))
    }
}

class ByteArrayDecoder extends OneToOneDecoder {
    override protected def decode(ctx: ChannelHandlerContext, channel: Channel, msg: Object): Object = {
        msg match {
            case buf: ChannelBuffer => toByteArray(buf)
            case _ =>  msg
        }
    }

    private def toByteArray(buf: ChannelBuffer): Array[Byte] = {
        if (buf.arrayOffset() == 0 && buf.readableBytes() == buf.capacity()) {
            buf.array()
        }
        else {
            val array = new Array[Byte](buf.readableBytes)
            buf.getBytes(0, array)
            array
        }
    }
}

class ByteArrayEncoder extends OneToOneEncoder {
    override protected def encode(ctx: ChannelHandlerContext, channel: Channel, msg: Object): Object = {
        msg match {
            case buf: Array[Byte] => ChannelBuffers.wrappedBuffer(buf)
            case _ => msg
        }
    }
}
