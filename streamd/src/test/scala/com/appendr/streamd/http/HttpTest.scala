package com.appendr.streamd.http

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.LoggerFactory
import com.appendr.streamd.network.services.{Service, HttpServices}
import com.appendr.streamd.network.netty.NettyHttpServer

/**
 * Created with IntelliJ IDEA.
 * User: bgordon
 * Date: 11/6/12
 * Time: 9:04 AM
 * To change this template use File | Settings | File Templates.
 */
@RunWith(classOf[JUnitRunner])
class HttpTest extends FunSuite with BeforeAndAfter {
    private val log = LoggerFactory.getLogger(getClass)
    val service = new HttpServices
    var server = NettyHttpServer(service)

    before {
        log.info("------- preparing to start -------")
        service.registerService(new Service() {
            def name = "test"
            def commands = List("hello")
            def command(cmd: Array[String]) = "Hello " + cmd(1)
            def commandHelp = List("hello says hello")
            def shutdown() {}
        })
    }

    test("server starts") {
        server.start(9999)
        Thread.sleep(10000)
        log.info("------- preparing to disconnect -------")
        server.stop()
        log.info("------- disconnected -------")
    }

    after {

    }
}