package com.appendr.streamd.cluster.zk

/**
 * Created by IntelliJ IDEA.
 * User: ben
 * Date: 12/4/11
 * Time: 2:37 PM
 * To change this template use File | Settings | File Templates.
 */

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory
import scala.collection.mutable
import com.appendr.streamd.conf.Configuration

@RunWith(classOf[JUnitRunner])
class ZKWatchTest extends FunSuite with BeforeAndAfter {
    private val log = LoggerFactory.getLogger(getClass)
    val config = Configuration.fromResource("base.conf")
    val map = Map.synchronized(new mutable.HashMap[String, String])
    val cb = (s: String) => {
        log.info(String.format("------- Node Changed CB: %s, %s", s, map.get(s)))
    }
    val ds = (ba: Array[Byte]) => {
        new String(ba)
    }

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

        // add watcher
        Client.client.watchAllChildren(map, ds, cb)

        Client.client.createEphemeral("crafter", "test".getBytes)
        Client.client.createEphemeral("crafters", "test".getBytes)

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