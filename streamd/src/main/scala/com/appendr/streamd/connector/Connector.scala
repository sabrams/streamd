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

import java.net.URI
import com.appendr.streamd.cluster.{Cluster, Node}
import com.appendr.streamd.conf.{ClusterSpec, Configuration}
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
    // TODO: Clean this up so it does not rely on config
    private val streamId = config.getString("streamd.client.streamid").get.hashCode

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
    protected override def connectorStart(args: Array[String]) {
        val iter = io.Source.fromFile(new URI(args.head)).getLines()
        for (s <- iter) post(s.getBytes)
    }

    protected override def connectorStop() {
    }
}

/**
 * Client that creates a local server to recieve callbacks
 * @param config configueration object
 * @param xfrm  transformer
 */
// TODO: Implement
class ClientConnector(config: Configuration, xfrm: InputTransformer[Array[Byte]])
    extends Connector[Array[Byte]](config, xfrm) {
    protected override def connectorStart(args: Array[String]) {

    }

    protected override def connectorStop() {
    }
}