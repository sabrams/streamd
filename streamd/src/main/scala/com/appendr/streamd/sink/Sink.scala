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
package com.appendr.streamd.sink

import com.appendr.streamd.conf.{Configuration, ConfigurableResource}
import java.io.{File, FileOutputStream}
import java.net.URI
import org.slf4j.LoggerFactory
import com.appendr.streamd.network.netty.NettyWebSocketClient

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

trait Sink extends ConfigurableResource {
    def out(msg: Any)
}

class FileSink extends Sink {
    private val log = LoggerFactory.getLogger(getClass)
    private var stream: Option[FileOutputStream] = None

    // TODO: externalize config strings with a spec
    def open(config: Option[Configuration]) {
        if (config.isDefined) {
            val path: Option[String] = config.get.getString("path")
            val f = new File(new URI(path.get))
            if (!f.exists()) f.createNewFile()
            f.setWritable(true)
            if (path.isDefined) stream = Some(new FileOutputStream(f))
        }
    }

    def close() {
        if (stream.isDefined) {
            stream.get.flush()
            stream.get.close()
        }
    }

    def out(msg: Any) {
        if (stream.isDefined) stream.get.write(msg.toString.getBytes)
        else log.warn("Stream is not open for writing.")
    }
}

class StdOutSink extends Sink {
    def close() {}
    def open(config: Option[Configuration]) {}
    def out(msg: Any) {
        System.out.println(msg)
    }
}

class WebSocketSink extends Sink {
    private var ws: Option[NettyWebSocketClient] = None
    def close() {
        if (ws.isDefined) ws.get.disconnect()
    }

    def open(config: Option[Configuration]) {
        val uri = URI.create(config.get.getString("uri").get)
        ws = Some(new NettyWebSocketClient(uri))
        ws.get.connect()
    }

    def out(msg: Any) {
        if (ws.isDefined) ws.get.send(msg.toString)
    }
}
