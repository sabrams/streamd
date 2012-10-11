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
import org.jboss.netty.handler.codec.serialization.{ClassResolvers, ObjectEncoder, ObjectDecoder}

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