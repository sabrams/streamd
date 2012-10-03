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
package com.appendr.streamd.stream.codec.kryo

import com.esotericsoftware.kryo.{Serializer, Kryo}
import com.esotericsoftware.kryo.io.{Input, Output}
import collection.immutable.HashMap

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class ScalaMapSerializer[K](kc: Class[K]) extends Serializer[Map[K, Any]] {
    def write (kryo: Kryo, o: Output, m: Map[K, Any]) {
        m.size match {
            case 0 => o.writeInt(m.size, true)
            case _ => {
                o.writeInt(m.size, true)
                m.foreach(writeEntry(kryo, o, _))
            }
        }
    }

    private def writeEntry(k: Kryo, o: Output, e: (K, Any)) {
        k.writeObject(o, e._1)
        k.writeClassAndObject(o, e._2)
    }

    def read(k: Kryo, i: Input, t: Class[Map[K, Any]]): Map[K, Any] = {
        var m: Map[K, Any] = k.newInstance(classOf[HashMap[K, Any]])
        val size = i.readInt(true)

        size match {
            case 0 => m
            case _ => {
                k.reference(m)
                for (_ <- 0 until size) {
                    m = HashMap[K, Any](k.readObject(i, kc) -> k.readClassAndObject(i)) ++ m
                }
                m.toMap
            }
        }
    }
}

