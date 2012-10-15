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
import com.appendr.streamd.component.{ClientComponentPool, ClientComponentAdaptor, ClientComponent}
import com.appendr.streamd.network.NoOpNetworkHandler
import com.appendr.streamd.util.{JMX, CounterMBean}
import java.util.concurrent.atomic.AtomicLong
import collection.mutable.ListBuffer

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object NettyClient {
    def apply[T](name: String, threaded: Boolean = false, poolCount: Int = 1): ClientComponent[T] = {
        val clients = new ListBuffer[ClientComponent[T]]()
        for (i <- 0 until poolCount) {
            if (threaded) clients += new ClientComponentAdaptor[T](new NettyClient(name))
            else clients += new NettyClient(name)
        }

        new ClientComponentPool[T](clients.toList)
    }
}

class NettyClient[T](val name: String) extends ClientComponent[T] with CounterMBean {
    private val log = LoggerFactory.getLogger(getClass)
    private val count = new AtomicLong(0L)
    private val lastCount = new AtomicLong(0L)
    @volatile private var channel: Channel = null
    private val bootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool,
            Executors.newCachedThreadPool))

    bootstrap.setPipelineFactory(ObjectPipelineFactory(new NoOpNetworkHandler))
    bootstrap.setOption("tcpNoDelay", true)
    bootstrap.setOption("keepAlive", true)

    def connect(address: InetSocketAddress) {
        if (channel == null || channel.isConnected == false) {
            val connectFuture = bootstrap.connect(address)
            channel = connectFuture.awaitUninterruptibly.getChannel
            JMX.register(this, getName())
        }
    }

    def disconnect() {
        channel.disconnect.awaitUninterruptibly
        JMX.unregister(this, getName())
    }

    def send(t: T) {
        if (log.isDebugEnabled) log.debug("Writing to channel: " + t)
        channel.write(t)
        count.incrementAndGet()
        lastCount.set(System.currentTimeMillis())
    }

    def getName() = "NettyClient-" + hashCode()
    def getCount() = count.longValue()
    def getTime() = lastCount.longValue()
}
