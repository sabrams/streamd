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
package com.appendr.streamd.cluster

import com.appendr.streamd.util.{Observable, ConsistentHash}
import scala.collection.mutable.HashMap

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

abstract class Topology extends Observable {
    private val ring = ConsistentHash[String](137)
    private val nodeMap = HashMap[String, Node]()

    def getNode(key: String): Node = {
        val nodeName = ring.getNode(key)
        nodeMap(nodeName)
    }

    def isThisNode(n: Node): Boolean
    def getNodes = nodeMap.values.toList
    override def toString: String = nodeMap.toString()

    /**
     * add a node
     */
    protected def +=(node: Node) {
        val decoded = NodeDecoder(node)
        nodeMap.put(decoded.name, node)
        ring += decoded.name
        notifyObservers
    }

    /**
     * remove a node
     */
    protected def -=(node: Node) = {
        val decoded = NodeDecoder(node)
        ring -= decoded.name
        nodeMap -= decoded.name
        notifyObservers
    }

    /**
     * remove a node
     */
    protected def -=(nodeName: String) = {
        ring -= nodeName
        nodeMap -= nodeName
        notifyObservers
    }
}
