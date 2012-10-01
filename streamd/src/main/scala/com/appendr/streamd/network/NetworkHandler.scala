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
import com.appendr.streamd.stream.codec.CodecFactory
import collection.mutable

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
    def apply(d: StreamRoutingDispatcher,f: CodecFactory[StreamEvent]) = {
        new DispatchingNetworkHandler(d, f)
    }
}

class DispatchingNetworkHandler(
    private val dispatcher: StreamRoutingDispatcher,
    private val factory: CodecFactory[StreamEvent])
    extends LoggingNetworkHandler {
    private val codec = factory()

    def handleMessage(msg: Object): Option[NetworkMessage] = {
        dispatcher.dispatch(codec.decode(msg.asInstanceOf[Array[Byte]]))
        None
    }
}

class NoOpNetworkHandler extends LoggingNetworkHandler {
    def handleMessage(msg: Object): Option[NetworkMessage] = {
        None
    }
}

class TelnetNetworkHandler extends LoggingNetworkHandler {
    private val map = new mutable.HashMap[String, TelnetPlugin]

    registerPlugin(new DefaultTelnetPlugin(map))

    def registerPlugin(tp: TelnetPlugin) {
        tp.commands.map(k => if (!map.contains(k)) map.put(tp.module + ":" + k, tp))
    }

    def handleMessage(msg: Object): Option[NetworkMessage] = {
        val m = msg.toString
        val t = map.get(m)
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

trait TelnetPlugin {
    val module: String
    def shutdown()
    def commands: List[String]
    def command(cmd: String): String
}

class DefaultTelnetPlugin(private val cmdMap: mutable.HashMap[String, TelnetPlugin])
    extends TelnetPlugin {
    val module = "streamd"

    cmdMap.put("help", this)

    def commands = List("help")
    def command(cmd: String) = {
        import com.appendr.streamd.util.RichCollection._
        cmd match {
            case "help" => {
                "To execute a command, module:command <args> \n\n -- Modules -- \n" +
                format(cmdMap.values.distinctBy(_.module).map(t => t.module + ":\n" + format(t.commands, true)))
            }
        }
    }

    def shutdown() {
    }

    private def format(list: Iterable[String], tab: Boolean = false): String = {
        val b = new StringBuilder
        for (s <- list) {
            if (tab) b.append("    ")
            b.append(s)
            b.append("\n")
        }

        b.toString()
    }
}



