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
import javax.management.ObjectInstance
import com.appendr.streamd.util.JMX._

@RunWith(classOf[JUnitRunner])
class JMXTest extends FunSuite {
    private var mbean: Option[ObjectInstance] = None
    test("Tests mbean registration") {

        mbean = JMX.register(new MBean(classOf[HelloMBean]) with HelloMBean {
           def sayHello() = "Hello!"
        })

        assert(JMX.isRegistered(mbean.get.getObjectName))

        JMX.unregister(mbean.get)

        assert(!JMX.isRegistered(mbean.get.getObjectName))
    }
}

trait HelloMBean{
    def sayHello(): String
}

