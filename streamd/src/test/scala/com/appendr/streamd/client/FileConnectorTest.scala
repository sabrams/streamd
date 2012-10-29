package com.appendr.streamd.client

/**
 * Created by IntelliJ IDEA.
 * User: ben
 * Date: 12/19/11
 * Time: 8:33 PM
 * To change this template use File | Settings | File Templates.
 */

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import com.appendr.streamd.conf.Configuration
import com.appendr.streamd.component._
import com.appendr.streamd.stream.StreamTuple
import com.appendr.streamd.connector.{ByteArrayToStringInput, FileConnector, InputTransformer}

@RunWith(classOf[JUnitRunner])
class FileConnectorTest extends FunSuite with BeforeAndAfter {
    val config = Configuration.fromResource("client.conf")
    var connector: FileConnector = null
    val config1 = Configuration.fromResource("server-a.conf")
    val config2 = Configuration.fromResource("server-b.conf")
    var node1: Server = null
    var node2: Server = null

    before {
        node1 = Server(config1)
        node1.start()
        node2 = Server(config2)
        node2.start()

        val ixf = new InputTransformer[String] {
            def transform(in: String): StreamTuple = {
                val v: Array[String] = in.split(",")
                val score: java.lang.Float = v(2).toFloat/10
                StreamTuple("colors", "color", Map[String, Object]("user"->v(0), "color"->v(1), "score"->score))
            }
        }

        connector = new FileConnector(config, ByteArrayToStringInput(ixf))
    }

    after {
        Thread.sleep(5000)
        connector.stop()
        node1.stop()
        node2.stop()
    }

    test("connector sends events") {
        connector.start(Array(getClass.getClassLoader.getResource("data.csv").toString))
    }
}

