/**
 * Created by IntelliJ IDEA.
 * User: ben
 * Date: 12/4/11
 * Time: 2:37 PM
 * To change this template use File | Settings | File Templates.
 */
package com.appendr.streamd.cluster.zk
import com.appendr.streamd.conf.Configuration
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory

@RunWith(classOf[JUnitRunner])
class ZKTest extends FunSuite with BeforeAndAfter {
    private val log = LoggerFactory.getLogger(getClass)
    val config = Configuration.fromResource("base.conf")

    object Client {
        var client: ZKClient = null
    }

    class TestConfigSpec extends ZKConfigSpec {
        hosts = config.apply("streamd.zookeeper.hosts")
        path = config.apply("streamd.zookeeper.path")

        log.info("------- path is set to: " + path.value)
        log.info("------- hosts is set to: " + hosts.value)
        timeout = 30000
    }

    before {
        log.info("------- preparing to connect -------")
        Client.client = new TestConfigSpec apply
    }

    test("ZKClient connects to ZooKeeper") {
        assert(Client.client.isAlive)
        log.info("------- connected -------")
        Thread.sleep(1000)
        Client.client.createBase
        Client.client.createEphemeral("n1", "test".getBytes)
        Client.client.createEphemeral("n2", "test".getBytes)
        Thread.sleep(4000)
        val seq = Client.client.getChildren
        for (c <- seq)
             log.info("------- child: " + c.toString)
        Thread.sleep(2000)
        Client.client.deleteRecursive()
        Thread.sleep(3000)
    }

    after {
        log.info("------- preparing to disconnect -------")
        Client.client.close()
        log.info("------- disconnected -------")
    }
}