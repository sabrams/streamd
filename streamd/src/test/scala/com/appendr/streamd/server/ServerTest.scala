
package com.appendr.streamd.server

/**
 * Created by IntelliJ IDEA.
 * User: ben
 * Date: 12/4/11
 * Time: 2:37 PM
 * To change this template use File | Settings | File Templates.
 */

import com.appendr.streamd.conf.Configuration
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, BeforeAndAfter}
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory
import com.appendr.streamd.network.netty.NettyTextServer
import com.appendr.streamd.network.TelnetNetworkHandler
import com.appendr.streamd.sink.Sink
import com.appendr.streamd.component.{ServerComponent, Server}
import com.appendr.streamd.stream.{StreamTuple, StreamProc}
import com.appendr.streamd.store.Store
import com.appendr.streamd.controlport.TelnetHandler

@RunWith(classOf[JUnitRunner])
class ServerTest extends FunSuite with BeforeAndAfter {
    private val log = LoggerFactory.getLogger(getClass)
    val config = Configuration.fromResource("server-a.conf")
    var server: Server = null
    var controlPort: ServerComponent = null

    before {
        log.info("------- preparing to start -------")
    }

    test("server starts") {
        val telnet = new TelnetNetworkHandler

        // start a control port
        controlPort = new NettyTextServer()
        controlPort.start(config.apply("streamd.control.port").toInt, telnet)

        server = Server(config)
        server.start()
        log.info("------- server has started: " + server.getNode.toString)

        Thread.sleep(10000)

        log.info("------- preparing to disconnect -------")
        if (controlPort != null) controlPort.stop()
        server.stop()
        log.info("------- disconnected -------")
    }

    after {

    }
}

class MyHandler extends TelnetHandler {
    val module = "test"

    def shutdown() {
    }

    def commands = List("test")

    def command(cmd: Array[String]) = {
        "Hello " + cmd.flatten
    }
}

class TestProc extends StreamProc {
    def coll(tuple: StreamTuple) {
        System.out.println("------> StreamProc collect tuple: " + tuple)
    }
    def proc(t: StreamTuple) = {
        System.out.println("======> StreamProc proccess tuple: " + t)
        Some(t)
    }

    def close() {}

    def open(config: Option[Configuration]) {}
}