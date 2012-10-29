package com.appendr.streamd.event

/**
 * Created by IntelliJ IDEA.
 * User: ben
 * Date: 12/19/11
 * Time: 8:33 PM
 * To change this template use File | Settings | File Templates.
 */

/*
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import com.appendr.streamd.conf.Configuration
import com.appendr.streamd.stream.{TwoWay, StreamProc, StreamTuple}
import com.appendr.streamd.connector.InputTransformer
import com.appendr.streamd.store.Store
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import io.Source


//@RunWith(classOf[JUnitRunner])
class EventProcessorTest extends FunSuite with BeforeAndAfter {
    val config1 = Configuration.fromResource("server-a.conf")
    val config2 = Configuration.fromResource("server-b.conf")
    val config3 = Configuration.fromResource("client.conf")
    var node1: Server = null
    var node2: Server = null
    var client: Client = null
    val ixf = new InputTransformer[String] {
        def transform(in: String): StreamTuple = {
            val v: Array[String] = in.split(",")
            val score: java.lang.Float = v(2).toFloat/10
            StreamTuple("event-test", "color", Map[String, Object]("user"->v(0), "color"->v(1), "score"->score))
        }
    }

    before {
        node1 = Server(config1)
        node1.start()
        node2 = Server(config2)
        node2.start()
        client = Client(config3)
        client.start()
    }

    after {
        Thread.sleep(5000)
        node1.stop()
        node2.stop()
        client.stop()
    }

    test("streamd processes events") {
        val dataStream = getClass.getClassLoader.getResourceAsStream("data.csv")
        val iter = Source.fromInputStream(dataStream).getLines()
        for (s <- iter) client.postEvent(ixf.transform(s), TwoWay)
    }
}

class ColorScore extends StreamProc with PluginContextAware {
    def proc(t: StreamTuple) = {
        val store = context._2.get
        val color: String = t._3.get("color").get.asInstanceOf[String]
        var count: Long = 0

        if (store.has(color)) {
            count = (store.get(color).asInstanceOf[AtomicLong].incrementAndGet())
        }
        else {
            store.set(color, new AtomicLong(1))
            count = 1
        }

        System.out.println("------ Mapping " + color + " has been seen " + count + " times.")

        if (count % 5 == 0)
            Some.apply(new StreamTuple(t._1, t._2, Map[String, String](t._2 -> color, "count" -> count.toString)))
        else
            None
    }

    def coll(t: StreamTuple) {
        val color = t._3.get("color").get
        val count = t._3.get("count").get
        System.out.println("++++++ Reducing " + color + " has been seen " + count + " times.")
    }

    def close() {}

    def open(config: Option[Configuration]) {}
}

class HashStore extends Store {
    private val map = new ConcurrentHashMap[String, AtomicLong]

    def set(key: String, value: Any) {
        map.put(key, value.asInstanceOf[AtomicLong])
    }

    def get(key: String) = {
        val r = map.get(key)
        if (r != null) Some(r)
        else None
    }

    def get(key: (String, String)) = {
        throw new UnsupportedOperationException
    }

    def rem(key: String) = {
        val r = map.remove(key)
        if (r != null) Some(r)
        else None
    }

    def has(key: String) = {
        map.containsKey(key)
    }

    def get(keys: String*) = {
        keys.map(s => {
            val r = map.get(s)
            if (r != null) Some(r)
            else None
        }).toList
    }

    def add(key: String, value: (_, Any)) {
        throw new UnsupportedOperationException
    }

    def rem(key: (String, String)) = {
        throw new UnsupportedOperationException
    }

    def has(key: (String, String)) = {
        throw new UnsupportedOperationException
    }

    def inc(key: String) {
        throw new UnsupportedOperationException
    }

    def inc(key: (String, String)) {
        throw new UnsupportedOperationException
    }

    def inc(key: String, i: Int) {
        throw new UnsupportedOperationException
    }

    def inc(key: (String, String), i: Int) {
        throw new UnsupportedOperationException
    }

    def close() {}

    def open(config: Option[Configuration]) {}

    def set(key: (String, String), value: Any) {
        throw new UnsupportedOperationException
    }
}*/