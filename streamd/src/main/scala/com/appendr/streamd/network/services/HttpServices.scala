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

import com.appendr.streamd.network.netty.NettyHttpNetworkHandler
import java.net.SocketAddress

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object HttpServices {
    def apply() = new HttpServices
}

class HttpServices extends NettyHttpNetworkHandler with Services[Service] {
    override def registerService(t: Service) {
        t.commands.map(k => if (!map.contains(k)) map.put(t.name + "/" + k, t))
    }

    def handleRequest(cmd: String) = map.contains(cmd)
    def handleRequest(cmd: String, path: Array[String]) = map.get(cmd).get.command(path)
    def handleConnect(address: SocketAddress) {}
    def handleDisconnect(address: SocketAddress) {}
}
