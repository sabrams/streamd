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
import jsr166y.ForkJoinPool
import com.appendr.streamd.cluster.{Router, Node}
import com.appendr.streamd.store.Store
import com.appendr.streamd.sink.Sink
import scala.collection
import collection.JavaConversions

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

case class StreamEvent(src: Source, stream: Option[StreamTuple])

trait StreamProc {
    def proc(t: StreamTuple, s: Option[Store], o: Option[Sink]): Option[StreamTuple]
    def coll(t: StreamTuple)
}

/**
 * StreamRoutingDispatcher delegates to configurable threaded stream dispatcher.
 */
object StreamRoutingDispatcher {
    def apply(p: StreamProc, r: Router, s: Option[Store], o: Option[Sink]) = {
        new StreamRoutingDispatcher(p, r, s, o)
    }
}

class StreamRoutingDispatcher(
    private val p: StreamProc,
    private val r: Router,
    private val s: Option[Store],
    private val o: Option[Sink]) {
    private val d: StreamDispatcher = new StreamDispatcher

    def start() {
        d.start()
    }

    def stop() {
        d.stop()
    }

    // Must execute in a separate thread
    def dispatch(e: StreamEvent) {
        d.dispatch(e, fn)
    }

    private def fn(e: StreamEvent) {
        import Lenses._
        e match {
            case StreamEvent(Source(_, _, OneWay), Some(x)) => p.proc(x, s, o)
            case StreamEvent(Source(_, _, Terminate), Some(x)) => p.coll(x)
            case StreamEvent(Source(_, _, Route), x) => r.route(e.src.node, stl.set(e, (sl.set(e.src, Terminate), x)))
            case StreamEvent(Source(_, _, TwoWay), Some(x)) => {
                p.proc(x, s, o) match {
                    case Some(res) => dispatch(stl.set(e, (sl.set(e.src, Route), Some(res))))
                    case None =>
                }
            }
            case _ =>
        }
    }

    private[StreamRoutingDispatcher] class StreamDispatcher {
        implicit var executor: ForkJoinPool = null

        def dispatch(e: StreamEvent, fn: (StreamEvent) => Unit) {
            val a = actor[(StreamEvent, (StreamEvent) => Unit)] {
                (m: (StreamEvent, (StreamEvent) => Unit)) => m._2(m._1)
            }

            a ! (e, fn)
        }

        def start() {
            executor = new ForkJoinPool
            (
                Runtime.getRuntime.availableProcessors,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                new Thread.UncaughtExceptionHandler {
                    def uncaughtException(t: Thread, e: Throwable) {
                        System.err.println(String.format("uncaughtException in thread '%s'", t))
                    }
                }, true
                )
        }

        def stop() {
            executor.shutdown()
        }
    }
}
