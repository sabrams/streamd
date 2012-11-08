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
import org.jboss.netty.handler.codec.frame.{LengthFieldPrepender, LengthFieldBasedFrameDecoder, DelimiterBasedFrameDecoder}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.handler.codec.serialization.{ObjectEncoder, ClassResolvers, ObjectDecoder}
import org.jboss.netty.handler.codec.http.{HttpResponseEncoder, HttpRequestDecoder}
import org.jboss.netty.handler.codec.oneone.{OneToOneEncoder, OneToOneDecoder}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object ObjectPipelineFactory {
    def apply(h: NetworkHandler) = {
        new ObjectPipelineFactory(h)
    }
}

class ObjectPipelineFactory(private val h: NetworkHandler)
    extends ChannelPipelineFactory {
    def getPipeline: ChannelPipeline = {
        Channels.pipeline(
            new ObjectDecoder(ClassResolvers.weakCachingConcurrentResolver(null)),
            new ObjectEncoder,
            new NettyHandler(h))
    }
}

object TextPipelineFactory {
    def apply(h: NetworkHandler) = {
        new TextPipelineFactory(h)
    }
}

class TextPipelineFactory(private val h: NetworkHandler)
    extends ChannelPipelineFactory {
    def getPipeline: ChannelPipeline = {
        Channels.pipeline(
            new DelimiterBasedFrameDecoder(8192,
                ChannelBuffers.wrappedBuffer(Array[Byte]('\r', '\n')),
                ChannelBuffers.wrappedBuffer(Array[Byte]('\n'))),
            new StringDecoder(CharsetUtil.UTF_8),
            new StringEncoder(CharsetUtil.UTF_8),
            new NettyHandler(h))
    }
}

object StringPipelineFactory {
    def apply(h: NetworkHandler) = {
        new StringPipelineFactory(h)
    }
}

class StringPipelineFactory(private val h: NetworkHandler)
    extends ChannelPipelineFactory {
    def getPipeline: ChannelPipeline = {
        Channels.pipeline(
            new StringDecoder,
            new StringEncoder,
            new NettyHandler(h))
    }
}

object HttpPipelineFactory {
    def apply(h: NetworkHandler) = {
        new HttpPipelineFactory(h)
    }
}

class HttpPipelineFactory(private val h: NetworkHandler)
    extends ChannelPipelineFactory {
    def getPipeline: ChannelPipeline = {
        Channels.pipeline(
            new HttpRequestDecoder,
            new HttpResponseEncoder,
            new NettyEventHandler(h))
    }
}

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
}


