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
package com.appendr.streamd.plugin

import collection.mutable

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

trait TelnetPlugin {
    def module: String
    def shutdown()
    def commands: List[String]
    def command(cmd: Array[String]): String
}

class DefaultTelnetPlugin(private val cmdMap: mutable.HashMap[String, TelnetPlugin])
    extends TelnetPlugin {
    def module: String = "streamd"

    cmdMap.put("help", this)

    def commands = List("help")
    def command(cmd: Array[String]) = {
        import com.appendr.streamd.util.RichCollection._
        cmd.head match {
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
