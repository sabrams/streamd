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
package com.appendr.streamd.connector

import java.util.UUID
import java.net.URI
import com.appendr.streamd.cluster.{Cluster, Node}
import com.appendr.streamd.conf.{ClusterSpec, Configuration}
import com.appendr.streamd.stream.codec.CodecFactory
import com.appendr.streamd.stream.StreamEvent
import com.appendr.streamd.stream.Source

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

/**
 * Base class for all connectors
 * @param config configueration object
 * @param xfrm  transformer
 * @tparam I  type to transform
 */
abstract class Connector[I](config: Configuration, xfrm: InputTransformer[I]) {
    private var cluster: Cluster = null
    private val streamId = UUID.randomUUID().toString

    val cs = new ClusterSpec(config)
    cs.validate()

    cluster = new Cluster(cs, None)

    final def start(args: Array[String]) {
        cluster.start()
        connectorStart(args)
    }

    final def stop() {
        connectorStop()
        cluster.stop()
    }

    final def post(msg: I) {
        val e = StreamEvent(Source(streamId, Node()), Some(xfrm.transform(msg)))
        val r = cluster.getRoute(e)
        if (r.isDefined) cluster.route(r.get, e)
    }

    protected def connectorStart(args: Array[String])
    protected def connectorStop()
}

/**
 * Basic streaming File Connector
 * @param config config object
 * @param xfrm transformer
 */
class FileConnector(config: Configuration, xfrm: InputTransformer[Array[Byte]])
    extends Connector[Array[Byte]](config, xfrm) {
    protected def connectorStart(args: Array[String]) {
        val iter = io.Source.fromFile(new URI(args.head)).getLines()
        for (s <- iter) post(s.getBytes)
    }

    protected def connectorStop() {
    }
}