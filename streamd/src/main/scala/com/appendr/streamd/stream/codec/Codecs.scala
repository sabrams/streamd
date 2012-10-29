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
package com.appendr.streamd.stream.codec

import com.appendr.streamd.stream._
import kryo.ScalaMapSerializer
import util.Marshal
import com.appendr.streamd.util.Reflector
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import java.io.ByteArrayOutputStream
import com.appendr.streamd.cluster.Node
import com.appendr.streamd.stream.StreamTuple
import com.appendr.streamd.stream.StreamEvent
import com.appendr.streamd.stream.Source

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

trait Codec[T] {
    def encode(t: T): Array[Byte]
    def decode(ba: Array[Byte]): T
}

object CodecFactory {
    def apply[T](s: String): CodecFactory[T] = {
        new CodecFactory[T](s)
    }
}

class CodecFactory[T](private val className: String) {
    def apply(): Codec[T] = {
        Reflector[Codec[T]](className)
    }
}

object KryoCodec {
    def apply() = {
        new KryoCodec
    }
}

class KryoCodec extends Codec[StreamEvent] {
    private val k = new Kryo
    private val ser  = new ScalaMapSerializer[String](classOf[String])
    def encode(e: StreamEvent): Array[Byte] = {
        val o = new Output(new ByteArrayOutputStream)
        o.writeInt(e.src.streamId)
        o.writeString(e.src.node.name)
        o.writeString(e.src.node.data)
        o.writeString(e.src.e.name)

        val t: StreamTuple = e.stream.get
        o.writeString(t._1)
        o.writeString(t._2)
        k.writeObject(o, t._3, ser)
        o.close()
        o.getBuffer
    }

    def decode(ba: Array[Byte]): StreamEvent = {
        val i = new Input(1)
        i.setBuffer(ba)
        val s = Source(i.readInt, Node(i.readString, i.readString), Exchange(i.readString))
        val _1 = i.readString()
        val _2 = i.readString()
        val map = k.readObject(i, classOf[Map[String, Object]], ser)
        val t = StreamTuple(_1, _2, map)
        new StreamEvent(s, Some(t))
    }
}

object JavaObjectCodec {
    def apply() = {
        new JavaObjectCodec
    }
}
class JavaObjectCodec extends Codec[StreamEvent] {
    def encode(e: StreamEvent): Array[Byte] = {
        Marshal.dump[StreamEvent](e)
    }

    def decode(ba: Array[Byte]): StreamEvent = {
        Marshal.load[StreamEvent](ba)
    }
}
