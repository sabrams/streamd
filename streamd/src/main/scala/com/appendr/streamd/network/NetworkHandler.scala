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
package com.appendr.streamd.network

import java.net.SocketAddress
import org.slf4j.LoggerFactory
import com.appendr.streamd.stream.{StreamEvent, StreamRoutingDispatcher}
import collection.mutable
import com.appendr.streamd.controlport.{DefaultTelnetHandler, TelnetHandler}
import com.appendr.streamd.util.{JMX, CounterMBean}
import java.util.concurrent.atomic.AtomicLong

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

trait NetworkHandler {
    def handleMessage(msg: Object): Option[NetworkMessage]
    def handleConnect(address: SocketAddress)
    def handleDisconnect(address: SocketAddress)
}

abstract class LoggingNetworkHandler extends NetworkHandler {
    protected val log = LoggerFactory.getLogger(getClass)

    def handleMessage(msg: Object): Option[NetworkMessage]
    def handleConnect(address: SocketAddress) {
        if (log.isInfoEnabled)
            log.info("---> received connection event address: " + address)
    }

    def handleDisconnect(address: SocketAddress) {
        if (log.isInfoEnabled)
            log.info("---> received disconnect event address: " + address)
    }
}

object DispatchingNetworkHandler {
    def apply(d: StreamRoutingDispatcher) = {
        new DispatchingNetworkHandler(d)
    }
}

class DispatchingNetworkHandler(private val dispatcher: StreamRoutingDispatcher)
    extends LoggingNetworkHandler with CounterMBean {
    private val count = new AtomicLong(0L)
    private val lastCount = new AtomicLong(0L)
    JMX.register(this, getName())

    def handleMessage(msg: Object): Option[NetworkMessage] = {
        dispatcher.dispatch(msg.asInstanceOf[StreamEvent])
        count.incrementAndGet()
        lastCount.set(System.currentTimeMillis())
        None
    }

    def getName() = "DispatchingNetworkHandler-" + hashCode()
    def getCount() = count.longValue()
    def getTime() = lastCount.longValue()
}

class NoOpNetworkHandler extends LoggingNetworkHandler {
    def handleMessage(msg: Object): Option[NetworkMessage] = {
        None
    }
}

class TelnetNetworkHandler extends LoggingNetworkHandler {
    private val map = new mutable.HashMap[String, TelnetHandler]

    registerPlugin(new DefaultTelnetHandler(map))

    def registerPlugin(tp: TelnetHandler) {
        tp.commands.map(k => if (!map.contains(k)) map.put(tp.module + ":" + k, tp))
    }

    def handleMessage(msg: Object): Option[NetworkMessage] = {
        val m = msg.toString.split(" ")
        val t = map.get(m.head)
        val message = t match {
            case None => Some(new NetworkMessage("bye!\n", ControlMessage.CLOSE))
            case _ => Some(new NetworkMessage(t.get.command(m), ControlMessage.REPLY))
        }

        message
    }

    override def handleDisconnect(address: SocketAddress) {
        super.handleConnect(address)
        map.map(e => e._2.shutdown())
        map.clear()
    }
}




