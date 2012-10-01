package com.appendr.streamd.util

/**
 * Created by IntelliJ IDEA.
 * User: ben
 * Date: 12/19/11
 * Time: 8:33 PM
 * To change this template use File | Settings | File Templates.
 */

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import com.appendr.streamd.conf.Configuration

@RunWith(classOf[JUnitRunner])
class ConfigTest extends FunSuite {
    test("Configuration reads config") {
        val config = Configuration.fromResource("server-a.conf")
        config.map.map(t => println(t._1 + " = " + t._2))
        assert(condition = true)
    }
}