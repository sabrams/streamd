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

import scala.collection.immutable.{TreeSet, Seq}
import scala.collection.mutable.{Buffer, Map}
import scala.util.MurmurHash

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object ConsistentHash {
    def apply[T](replicas: Int) = {
        new ConsistentHash[T](replicas)
    }
}

class ConsistentHash[T](private val replicas: Int) {
    private val delimiter = "-"
    private val cluster = Buffer[T]()
    private var keys = TreeSet[Long]()
    private var ring = Map[Long, T]()

    def this(nodes: Seq[T], replicas: Int) {
        this(replicas: Int)
        nodes.foreach(this += _)
    }

    def +=(node: T) {
        if (!cluster.contains(node)) {
            cluster += node
            (1 to replicas) foreach {
                replica =>
                    val key = hashFor((node + delimiter + replica).getBytes("UTF-8"))
                    ring += (key -> node)
                    keys += key
            }
        }
    }

    def -=(node: T) {
        cluster -= node
        (1 to replicas) foreach {
            replica =>
                val key = hashFor((node + delimiter + replica).getBytes("UTF-8"))
                ring -= key
                keys -= key
        }
    }

    def getNode(key: String): T = {
        val bkey = key.getBytes("UTF-8")
        val hash = hashFor(bkey)
        if (keys contains hash) {
            ring(hash)
        }
        else {
            if (hash < keys.firstKey)
                ring(keys.firstKey)
            else if (hash > keys.lastKey)
                ring(keys.lastKey)
            else
                ring(keys.rangeImpl(None, Some(hash)).lastKey)
        }
    }

    private def hashFor(bytes: Array[Byte]): Long = {
        val hash = MurmurHash.arrayHash(bytes)
        if (hash == Int.MinValue) hash + 1
        math.abs(hash)
    }
}