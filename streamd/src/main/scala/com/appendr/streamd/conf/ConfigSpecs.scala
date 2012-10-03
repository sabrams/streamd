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

import java.net.InetSocketAddress
import com.appendr.streamd.cluster.zk.ZKConfigSpec
import com.appendr.streamd.stream.{StreamEvent, StreamProc}
import com.appendr.streamd.cluster.Node
import com.appendr.streamd.util.Reflector
import com.appendr.streamd.stream.codec.CodecFactory
import com.appendr.streamd.store.Store
import com.appendr.streamd.sink.Sink
import com.appendr.streamd.plugin.StreamPlugin

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

sealed class ClusterSpec(config: Configuration) extends ZKConfigSpec {
    hosts = config.apply("streamd.zookeeper.hosts")
    path = config.apply("streamd.zookeeper.path")
    timeout = config.apply("streamd.zookeeper.timeout").toInt
}

abstract sealed class BaseConfigSpec[T] extends ConfigSpec[T] {
    var name = required[String]
    var port = required[Int]
    var codec = required[CodecFactory[StreamEvent]]
}

abstract sealed class BaseConfig[T](val spec: BaseConfigSpec[T]) {
    val address = new InetSocketAddress(spec.port.value)
    val codec = spec.codec
    val node: Option[Node]
}

sealed class ServerSpec(config: Configuration) extends ServerConfigSpec {
    name = config.apply("streamd.server.name")
    port = config.apply("streamd.server.port").toInt
    codec = CodecFactory[StreamEvent](config.apply("streamd.codec"))
}

sealed class ClientSpec(config: Configuration) extends ClientConfigSpec {
    name = config.apply("streamd.client.name")
    port = config.apply("streamd.client.port").toInt
    codec = CodecFactory[StreamEvent](config.apply("streamd.codec"))
}

sealed class ServerConfigSpec extends BaseConfigSpec[ServerConfig] {
    def apply() = new ServerConfig(this)
}

sealed class ClientConfigSpec extends BaseConfigSpec[ClientConfig] {
    def apply() = new ClientConfig(this)
}

sealed class ServerConfig(override val spec: ServerConfigSpec) extends BaseConfig[ServerConfig](spec) {
    override val node = Some(Node(spec.name.value, address.getHostName, address.getPort))
}

sealed class ClientConfig(override val spec: ClientConfigSpec) extends BaseConfig[ClientConfig](spec) {
    override val node = Some(Node(spec.name.value, address.getHostName, address.getPort, routable = false))
}

abstract sealed class PluginConfigSpec extends ConfigSpec[StreamPlugin] {
    var proc = required[StreamProc]
    var sink = optional[Sink]
    var store = optional[Store]
}

sealed class PluginSpec(config: Configuration) extends PluginConfigSpec {
    proc = Reflector[StreamProc](config.apply("streamd.plugin.processor.class"))

    val sinkClass = config.getString("streamd.plugin.sink.class")
    sink = {
        if (sinkClass.isDefined) Some(Reflector[Sink](sinkClass))
        else None
    }

    val storeClass = config.getString("streamd.plugin.store.class")
    store = {
        if (sinkClass.isDefined) Some(Reflector[Store](storeClass))
        else None
    }

    def apply() = {
        val plugin = StreamPlugin(proc, sink, store)
        plugin.open(config)
        plugin
    }
}
