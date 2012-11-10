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
package com.appendr.streamd.conf

import java.net.{NetworkInterface, InetSocketAddress}
import com.appendr.streamd.cluster.zk.ZKConfigSpec
import com.appendr.streamd.cluster.Node
import com.appendr.streamd.util.{ExternalIp, PortScanner, Reflector}
import com.appendr.streamd.module.Module

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sealed class ClusterSpec(config: Configuration) extends ZKConfigSpec {
    hosts = config.apply("streamd.zookeeper.hosts")
    path = config.apply("streamd.zookeeper.path")
    timeout = config.apply("streamd.zookeeper.timeout").toInt
}

abstract sealed class BaseConfigSpec[T] extends ConfigSpec[T] {
    var name = required[String]
    var port = required[Int]
    var cport = required[Int]
}

abstract sealed class BaseConfig[T](val spec: BaseConfigSpec[T]) {
    val address = new InetSocketAddress(ExternalIp.getFirstInetAddress(), spec.port.value)
    val node: Option[Node]
}

sealed class ServerSpec(config: Configuration) extends ServerConfigSpec {
    name = config.apply("streamd.server.name")
    port = config.apply("streamd.server.port").toInt
    cport = config.apply("streamd.server.control.port").toInt
}

sealed class ServerConfigSpec extends BaseConfigSpec[ServerConfig] {
    def apply() = new ServerConfig(this)
}

sealed class ServerConfig(override val spec: ServerConfigSpec) extends BaseConfig[ServerConfig](spec) {
    override val node = {
        val mport = PortScanner.scan(Range(10000, 32767))
        Some(Node(spec.name.value, address.getHostName, address.getPort, mport.get))
    }
}

abstract sealed class ModuleConfigSpec extends ConfigSpec[List[Module]] {
    var classes = required[Seq[String]]
}

sealed class ModuleSpec(config: Configuration) extends ModuleConfigSpec {
    classes = config.getList("streamd.modules")
    def apply() = {
        val modules = classes.map(s => Reflector[Module](s)).toList
        modules.foreach(
            m => {
                if (m.sink.isDefined) m.proc.moduleContext = (m.sink, None)
                if (m.store.isDefined) m.proc.moduleContext = (m.proc.moduleContext._1, m.store)
            }
        )

        modules
    }
}
