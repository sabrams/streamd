
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
import com.appendr.streamd.component.Server

@RunWith(classOf[JUnitRunner])
class ServerTest extends FunSuite with BeforeAndAfter {
    private val log = LoggerFactory.getLogger(getClass)
    val config = Configuration.fromResource("server-b.conf")
    var server: Server = null

    before {
        log.info("------- preparing to start -------")
    }

    test("server starts") {
        server = Server(config)
        server.start()
        log.info("------- server has started: " + server.getNode.toString)

        Thread.sleep(10000)

        log.info("------- preparing to disconnect -------")
        server.stop()
        log.info("------- disconnected -------")
    }

    after {

    }
}
