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
package com.appendr.streamd.network.services

import com.appendr.streamd.network.{ControlMessage, NetworkMessage, LoggingNetworkHandler}
import collection.mutable

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class DefaultTelnetService(private val cmdMap: mutable.HashMap[String, Service])
    extends Service {
    def name: String = "streamd"

    cmdMap.put("help", this)

    def commands = List("help")
    def commandHelp = List("help - prints this message")
    def command(cmd: Array[String]) = {
        import com.appendr.streamd.util.RichCollection._
        cmd.head match {
            case "help" => {
                "To execute a command, module:command <args> \n\n -- Modules -- \n" +
                    format(cmdMap.values.distinctBy(_.name).map(t => t.name + ":\n" + format(t.commandHelp, true)))
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

object TelnetServices {
    def apply() = new TelnetServices
}

class TelnetServices extends LoggingNetworkHandler with Services[Service] {
    registerService(new DefaultTelnetService(map))

    def handleMessage(msg: Object): Option[NetworkMessage] = {
        val m = msg.toString.split(" ")
        val t = map.get(m.head)
        val c = m.head.split(":")
        val message = t match {
            case None => {
                c.head match {
                    case "quit" => Some(new NetworkMessage("bye!\n", ControlMessage.CLOSE))
                    case _ => Some(new NetworkMessage("unknown command, try \"help\"\n", ControlMessage.REPLY))
                }
            }
            case _ => {
                c.head match {
                    case "help" => Some(new NetworkMessage(t.get.command(c), ControlMessage.REPLY))
                    case _ => Some(new NetworkMessage(t.get.command(c.tail ++ m.tail), ControlMessage.REPLY))
                }
            }
        }

        message
    }
}
