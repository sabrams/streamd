package com.appendr.streamd.sink

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.net.URI

@RunWith(classOf[JUnitRunner])
class FileSinkTest extends FunSuite with BeforeAndAfter {
    val uri = Thread.currentThread().getContextClassLoader.getResource("base.conf")
    private val sink = new FileSink(uri.getPath, "test.txt")

    before {
        sink.open()
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
    private val sink = new WebSocketSink(new URI("ws://localhost:8080"))

    before {
        sink.open()
    }

    after {
        sink.close()
    }

    test("write to sink") {
        sink.out("Hello World!")
        Thread.sleep(1000)
    }
}