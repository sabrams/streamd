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
package com.appendr.streamd.proc.cep.telnet

import com.appendr.streamd.network.TelnetPlugin

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class ControlPort extends TelnetPlugin {
    val module = "CEP"
    def commands = List("info", "list", "load", "unload", "activate")
    def command(cmd: Array[String]) = {
        cmd.head match {
            case "info" => doInfo(cmd.tail)
            case "list" => doList(cmd.tail)
            case "load" => doLoad(cmd.tail)
            case "unload" => doUnload(cmd.tail)
            case "activate" => doActivate(cmd.tail)
            case _ => "Unknown command: " + cmd.head
        }
    }

    def shutdown() {
    }

    private def doInfo(cmd: Array[String]) = {
        "Not implemented"
    }

    private def doList(cmd: Array[String]) = {
        "Not implemented"
    }

    private def doLoad(cmd: Array[String]) = {
        "Not implemented"
    }

    private def doUnload(cmd: Array[String]) = {
        "Not implemented"
    }

    private def doActivate(cmd: Array[String]) = {
        "Not implemented"
    }
}
