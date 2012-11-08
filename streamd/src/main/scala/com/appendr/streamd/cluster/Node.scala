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

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object Node {
    def apply() = new EmptyNode
    def apply(name: String, host: String, port: Int, mport: Int, routable: Boolean = true) =
        new Node(name, host + ":" + port + ":" + mport + ":" + routable.toString)
    def apply(name: String, data: String) = new Node(name, data)
}

@SerialVersionUID(1)
class Node(val name: String, val data: String = "") extends Serializable {
    override def toString = "Node{" + "name=" + name.toString + " data=" + data.toString + '}'
    override def hashCode = 41 * super.hashCode + name.hashCode + data.hashCode
    def canEqual(other: Any) = other.isInstanceOf[Node]
    override def equals(other: Any) = other match {
        case that: Node =>
            (that canEqual this) && this.name == that.name && this.data == that.data
        case that: Option[Node] =>
            (that.isDefined) && (that.get canEqual this) &&
            this.name.equals(that.get.name) && this.data.equals(that.get.data)
        case _ =>
            false
    }
}

@SerialVersionUID(1)
case class EmptyNode() extends Node("", "")

object NodeDecoder {
    def apply(node: Node) = {
        new NodeDecoder(node)
    }
}

@SerialVersionUID(1)
class NodeDecoder(val node: Node) extends Serializable {
    val name = node.name
    val hpe = parse(node)

    def host = hpe._1
    def port = hpe._2
    def managementPort = hpe._3
    def isRoutable = hpe._4

    private def parse(node: Node): (String, Int, Int, Boolean) = {
        val n = node.data.split(':')
        (n(0), n(1).toInt, n(2).toInt, n(3).toBoolean)
    }

    override def toString =
        "NodeDecoder{" + "name=" + name + " host=" + hpe._1 + " port=" + hpe._2 + " managementPort=" + hpe._3 + "routable=" + hpe._4 + '}'
}