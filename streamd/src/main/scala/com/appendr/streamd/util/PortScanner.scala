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
package com.appendr.streamd.util

import java.net.{InetAddress, NetworkInterface, ServerSocket}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object PortScanner {
    def scan(range: Range): Option[Int] = {
        def open(port: Int): Boolean = {
            try {
                val s = new ServerSocket(port)
                s.close()
                true
            }
            catch { case e => false }
        }

        range.collectFirst { case (port) if (open(port)) => port }
    }
}

object ExternalIp {
    def getFirstInetAddress(): InetAddress = {
        import collection.JavaConverters._
        val nics = NetworkInterface.getNetworkInterfaces.asScala
        val ifs = nics.filter(nic => (!nic.isLoopback && !nic.isPointToPoint && !nic.isVirtual && nic.isUp))
        val ips = ifs.map(i => i.getInetAddresses.asScala.filter(inet => inet.isReachable(5))).flatten.toList

        if (ips.isEmpty) InetAddress.getLocalHost
        else ips.head
    }
}
