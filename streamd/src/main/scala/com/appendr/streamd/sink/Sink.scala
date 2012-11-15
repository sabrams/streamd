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

import com.appendr.streamd.util.LifeCycle
import java.io.{BufferedWriter, FileWriter, File, FileOutputStream}
import java.net.URI
import org.slf4j.LoggerFactory
import com.appendr.streamd.network.netty.NettyWebSocketClient

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

trait Sink extends LifeCycle {
    def out(msg: Any)
}

class FileSink(path: String, name: String) extends Sink {
    private val log = LoggerFactory.getLogger(getClass)
    private var stream: Option[BufferedWriter] = None

    def open() {
        val f = new File(path, name)
        if (!f.exists()) f.createNewFile()
        f.setWritable(true)
        stream = Some(new BufferedWriter(new FileWriter(f)))
    }

    def close() {
        if (stream.isDefined) {
            stream.get.flush()
            stream.get.close()
        }
    }

    def out(msg: Any) {
        if (stream.isDefined) {
            stream.get.write(msg.toString)
            stream.get.newLine()
        }
        else log.warn("Sink is not open for writing.")
    }
}

class StdOutSink extends Sink {
    def close() {}
    def open() {}
    def out(msg: Any) {
        System.out.println(msg)
    }
}

class WebSocketSink(uri: URI) extends Sink {
    private var ws: Option[NettyWebSocketClient] = None
    def close() {
        if (ws.isDefined) ws.get.disconnect()
    }

    def open() {
        ws = Some(new NettyWebSocketClient(uri))
        ws.get.connect()
    }

    def out(msg: Any) {
        if (ws.isDefined) ws.get.send(msg.toString)
    }
}
