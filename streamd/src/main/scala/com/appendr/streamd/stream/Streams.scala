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
package com.appendr.streamd.stream

import scalaz._
import Scalaz._
import scala.collection
import collection.JavaConversions
import com.appendr.streamd.cluster.{Router, Node}
import com.appendr.streamd.conf.ConfigurableResource
import com.appendr.streamd.util.{JMX, CounterMBean}
import java.util.concurrent.atomic.AtomicLong
import com.appendr.streamd.util.threading.ForkJoinStrategy

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

/**
 * Various Lenses to compose StreamEvents
 */
object Lenses {
    private val el: Lens[StreamEvent, Source] =
        Lens((_: StreamEvent).src, (e: StreamEvent, s: Source) => e.copy(src = s))
    private val tl: Lens[StreamEvent, Option[StreamTuple]] =
        Lens((_: StreamEvent).stream,(e: StreamEvent, t: Option[StreamTuple]) => e.copy(stream = t))
    val sl: Lens[Source, Exchange] =
        Lens((_: Source).e, (src: Source, ex: Exchange) => src.copy(e = ex))
    val stl: Lens[StreamEvent, (Source, Option[StreamTuple])] = Lens((e: StreamEvent) => (el.get(e), tl.get(e)),
        (e: StreamEvent, t: (Source, Option[StreamTuple])) => (tl.set(el.set(e, t._1), t._2)))
}

/**
 * Describes a message exchange pattern.
 */
object Exchange {
    def apply(s: String): Exchange = {
        s match {
            case OneWay.name => OneWay
            case TwoWay.name => TwoWay
            case Route.name => Route
            case _ => Terminate
        }
    }
}

/**
 * Message exchange patterns
 */
sealed trait Exchange {
    def name: String
}

case object OneWay extends Exchange { val name = "OneWay" }
case object TwoWay extends Exchange { val name = "TwoWay" }
case object Route extends Exchange { val name = "Route" }
case object Terminate extends Exchange { val name = "Terminate" }

case class Source(streamId: String, node: Node, var e: Exchange = OneWay)

/**
 * Give a generic Tuple a name as opposed to referencing (String, String, Map[String, Object])
 * @param _1 tuple name
 * @param _2 routing key extracted from map
 * @param _3 key value attributes
 */
case class StreamTuple(override val _1: String, override val _2: String, override val _3: Map[String, Object])
    extends (String, String, Map[String, Object])(_1, _2, _3) {
    def this(name: String, key: String, m: collection.mutable.Map[String, Object]) = {
        this(name, key, m.toMap)
    }

    def this(name: String, key: String, m: java.util.Map[String, Object]) = {
        this(name, key, JavaConversions.mapAsScalaMap(m))
    }
}

/**
 * Framework Event
 * @param src Source routing information
 * @param stream Option[StreamTuple] carries data payload
 */
case class StreamEvent(src: Source, stream: Option[StreamTuple])

/**
 * Trait for custom stream preocessors
 */
trait StreamProc extends ConfigurableResource {
    def proc(t: StreamTuple): Option[StreamTuple]
    def coll(t: StreamTuple)
}

/**
 * StreamRoutingDispatcher delegates to threaded stream dispatcher.
 */
object StreamRoutingDispatcher {
    def apply(p: StreamProc, r: Router) = {
        new StreamRoutingDispatcher(p, r)
    }
}

/**
 * Actor dispatcher
 * @param p the streamproc
 * @param r the router
 */
class StreamRoutingDispatcher(
    private val p: StreamProc,
    private val r: Router) extends CounterMBean {
    private val count = new AtomicLong(0L)
    private val lastCount = new AtomicLong(0L)
    private val a = actor[StreamEvent](
    e => {
        import Lenses._
        e match {
            case StreamEvent(Source(_, _, OneWay), Some(x)) => p.proc(x)
            case StreamEvent(Source(_, _, Terminate), Some(x)) => p.coll(x)
            case StreamEvent(Source(_, _, Route), x) => r.route(e.src.node, stl.set(e, (sl.set(e.src, Terminate), x)))
            case StreamEvent(Source(_, _, TwoWay), Some(x)) => {
                p.proc(x) match {
                    case Some(res) => dispatch(stl.set(e, (sl.set(e.src, Route), Some(res))))
                    case None =>
                }
            }
            case _ =>
        }
    })(ForkJoinStrategy.strategy)

    def start() {
        JMX.register(this, getName())
    }

    def stop() {
        JMX.unregister(this, getName())
    }

    def dispatch(e: StreamEvent) {
        actor(a) ! e
        count.incrementAndGet()
        lastCount.set(System.currentTimeMillis())
    }

    def getName() = "StreamRoutingDispatcher-" + this.hashCode()
    def getCount() = count.longValue()
    def getTime() = lastCount.longValue()
}

