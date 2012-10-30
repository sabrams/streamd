/**
 * Copyright (C) 2011 apendr.com
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 *   _  _  _  _  _  _| _
 *  (_||_)|_)(/_| |(_||
 *     |  |
 */
package com.appendr.streamd.util

import java.lang.management.ManagementFactory
import javax.management.{MXBean, StandardMBean, ObjectName, ObjectInstance}
import org.slf4j.LoggerFactory
import java.util.Hashtable

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object JMX {
    private val server = ManagementFactory.getPlatformMBeanServer
    private val log = LoggerFactory.getLogger(getClass)

    def register(bean: MBean): Option[ObjectInstance] = register(bean, bean.name)

    def register(bean: AnyRef, name: String): Option[ObjectInstance] = {
        try {
            Some(server.registerMBean(bean, objectNameForBean(bean, name)))
        }
        catch {
            case ex: Exception =>
                log.error("Failed to register mbean: %s".format(bean), ex)
                None
        }
    }

    def unregister(bean: ObjectInstance) {
        try {
            server.unregisterMBean(bean.getObjectName)
        }
        catch {
            case ex: Exception => log.error("Failed to unregister mbean: %s".format(bean.getObjectName), ex)
        }
    }

    def unregister(bean: AnyRef, name: String) {
        try {
            server.unregisterMBean(objectNameForBean(bean, name))
        }
        catch {
            case ex: Exception => log.error("Failed to unregister mbean: %s".format(name), ex)
        }
    }

    def isRegistered(name: ObjectName): Boolean = {
        server.isRegistered(name)
    }

    def objectNameForBean(bean: AnyRef, name: String): ObjectName = {
        val ht = new Hashtable[String, String]
        ht.put("class", bean.getClass.getName)
        ht.put("type", bean.getClass.getSimpleName)
        ht.put("name", name)
        new ObjectName(bean.getClass.getPackage.getName, ht)
    }

    class MBean(clazz: Class[_]) extends StandardMBean(clazz) {
        def name: String = {
            val name = clazz.getSimpleName
            val index = name.lastIndexOf("MBean")
            "com.appendr.streamd.%s".format(if (index == -1) name else name.substring(0, index))
        }
    }
}

@MXBean
trait CounterMBean {
    def getName(): String
    def getCount(): Long
    def getTime(): Long
}

@MXBean
trait ListableMBean {
    def getList(): Array[String]
}

@MXBean
trait QueuedCountMBean {
    def getQueuedCount(): Int
}

@MXBean
trait ListableCounterMBean extends CounterMBean with ListableMBean

@MXBean
trait QueuedCounterMBean extends CounterMBean with QueuedCountMBean