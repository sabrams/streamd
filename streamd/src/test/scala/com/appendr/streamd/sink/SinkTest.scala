package com.appendr.streamd.sink

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.appendr.streamd.conf.Configuration

@RunWith(classOf[JUnitRunner])
class FileSinkTest extends FunSuite with BeforeAndAfter {
    private val sink = new FileSink

    before {
        val conf = Configuration.fromResource("test.conf")
        sink.open(conf.getSection("streamd.plugin.sink"))
    }

    after {
        sink.close()
    }

    test("write to sink") {
        sink.out("Hello World!")
    }
}

//@RunWith(classOf[JUnitRunner])
class WebSocketSinkTest extends FunSuite with BeforeAndAfter {
    private val sink = new WebSocketSink

    before {
        val conf = Configuration.fromResource("test.conf")
        sink.open(conf.getSection("streamd.plugin.sink"))
    }

    after {
        sink.close()
    }

    test("write to sink") {
        sink.out("Hello World!")
        Thread.sleep(1000)
    }
}