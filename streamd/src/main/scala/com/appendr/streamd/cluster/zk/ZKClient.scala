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
package com.appendr.streamd.cluster.zk

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.immutable.Set
import org.apache.zookeeper.{ CreateMode, KeeperException, Watcher, WatchedEvent, ZooKeeper }
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.Watcher.Event.KeeperState
import java.util.concurrent.CountDownLatch
import org.slf4j.LoggerFactory

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class ZKClient(
        servers: String, 
        timeout: Int, 
        basePath: String, 
        watcher: Option[ZKClient => Unit]) {
    
    private val log = LoggerFactory.getLogger(getClass)
    @volatile private var zk: ZooKeeper = null

    def this(spec: ZKConfigSpec, watch: Option[ZKClient => Unit]) =
        this(spec.hosts.value, spec.timeout.value, spec.path.value, watch)

    def this(servers: String, timeout: Int, basePath: String) =
        this(servers, timeout, basePath, None)

    def this(servers: String, timeout: Int, basePath: String, watch: (ZKClient => Unit)) =
        this(servers, timeout, basePath, Some(watch))

    def this(spec: ZKConfigSpec) =
        this(spec, None)

    connect()

    private def connect() {
        val connectionLatch = new CountDownLatch(1)
        val assignLatch = new CountDownLatch(1)
        val watcher = new Watcher {
            def process(event: WatchedEvent) {
                sessionEvent(assignLatch, connectionLatch, event)
            }
        }
        
        if (zk != null) {
            zk.close()
            zk = null
        }
        
        zk = new ZooKeeper(servers, timeout, watcher)
        
        assignLatch.countDown()

        if (log.isInfoEnabled)
            log.info("--- Attempting to connect to zookeeper servers: " + servers)

        connectionLatch.await()
    }

    def sessionEvent(assignLatch: CountDownLatch, connectionLatch: CountDownLatch, event: WatchedEvent) {
        assignLatch.await()
        event.getState match {
            case KeeperState.SyncConnected => {
                try {
                    watcher.map(fn => fn(this))
                    if (log.isInfoEnabled)
                        log.info("--- Zookeeper event: %s".format(event))

                } catch {
                    case e: Exception =>
                        log.error("--- Exception during zookeeper connection established callback" + e.getMessage)
                }
                connectionLatch.countDown()
            }
            case KeeperState.Expired => {
                connect()
                if (log.isInfoEnabled)
                    log.info("--- Zookeeper event: %s".format(event))
            }
            case _ =>
        }
    }

    def getChildren(path: String): Seq[String] = {
        zk.getChildren(makeNodePath(path), false)
    }

    def getChildren: Seq[String] = {
        zk.getChildren(basePath, false)
    }

    def close() {
        if (log.isInfoEnabled)
            log.info("--- Zookeeper closing.")
        zk.close()
    }

    def isAlive: Boolean = {
        // If you can get the root, then we're alive.
        val result: Stat = zk.exists("/", false)
        result.getVersion >= 0
    }

    def createBase: String = {
        try {
            zk.create(basePath, Array[Byte]{0}, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        }
        catch {
            case e: Exception => {
                if (log.isInfoEnabled)
                    log.info(e.toString)
            ""
            }
        }
    }

    def create(path: String, data: Array[Byte], createMode: CreateMode): String = {
        zk.create(makeNodePath(path), data, Ids.OPEN_ACL_UNSAFE, createMode)
    }

    def createEphemeral(path: String, data: Array[Byte]): String = {
        create(path, data, CreateMode.EPHEMERAL)
    }

    def createPersistent(path: String, data: Array[Byte]): String = {
        create(path, data, CreateMode.PERSISTENT)
    }

    def get(path: String): Array[Byte] = {
        zk.getData(makeNodePath(path), false, null)
    }

    def set(path: String, data: Array[Byte]) {
        zk.setData(makeNodePath(path), data, -1)
    }

    def delete(path: String) {
        zk.delete(makeNodePath(path), -1)
    }

    def deleteRecursive(path: String = "") {
        val children = getChildren(path)

        for (node <- children) {
            deleteRecursive(path + '/' + node)
        }

        delete(path)
    }

    def watchNode(node: String, onDataChanged: Option[Array[Byte]] => Unit) {
        val path = makeNodePath(node)
        if (log.isInfoEnabled)
            log.info("--- Watching node: " + path)

        def updateData() {
            try {
                onDataChanged(Some(zk.getData(path, dataWatcher, null)))
            }
            catch {
                case e: KeeperException => {
                    log.warn(String.format("--- Failed to read node %s: %s", node, e))
                    deletedData()
                }
            }
        }

        def deletedData() {
            onDataChanged(None)
            if (zk.exists(path, dataWatcher) != null) updateData()
        }

        def dataWatcher = new Watcher {
            def process(event: WatchedEvent) {
                if (event.getType == EventType.NodeDataChanged ||
                    event.getType == EventType.NodeCreated) {
                    updateData()
                } else if (event.getType == EventType.NodeDeleted) {
                    deletedData()
                }
            }
        }

        updateData()
    }

    def watchChildren(node: String, updateChildren: Seq[String] => Unit) {
        val path = makeNodePath(node)
        val childWatcher = new Watcher {
            def process(event: WatchedEvent) {
                if (event.getType == EventType.NodeChildrenChanged ||
                    event.getType == EventType.NodeCreated) {
                    watchChildren(node, updateChildren)
                }
            }
        }
        try {
            val children = zk.getChildren(path, childWatcher)
            updateChildren(children)
        } catch {
            case e: KeeperException => {
                log.warn(String.format("--- Failed to read node %s: %s", path, e))
                updateChildren(List())
                zk.exists(path, childWatcher)
            }
        }
    }

    def watchAllChildren[T](
        watchMap: mutable.Map[String, T],
        deserialize: Array[Byte] => T,
        notifier: String => Unit) {
        watchChildrenWithData("", watchMap, deserialize, notifier)
    }

    def watchChildrenWithData[T](
        node: String,
        watchMap: mutable.Map[String, T],
        deserialize: Array[Byte] => T) {
        watchChildrenWithData(node, watchMap, deserialize, None)
    }

    def watchChildrenWithData[T](
        node: String,
        watchMap: mutable.Map[String, T],
        deserialize: Array[Byte] => T,
        notifier: String => Unit) {
        watchChildrenWithData(node, watchMap, deserialize, Some(notifier))
    }

    private def makeNodePath(path: String) = {
        if (path.isEmpty) basePath else "%s/%s".format(basePath, path).replaceAll("//", "/")
    }

    private def watchChildrenWithData[T](
        node: String,
        watchMap: mutable.Map[String, T],
        deserialize: Array[Byte] => T,
        notifier: Option[String => Unit]) {

        def nodeChanged(child: String)(childData: Option[Array[Byte]]) {
            childData match {
                case Some(data) => {
                    watchMap.synchronized {
                        watchMap(child) = deserialize(data)
                    }
                    notifier.map(f => f(child))
                }
                case None =>
            }
        }

        def parentWatcher(children: Seq[String]) {
            val childrenSet = Set(children: _*)
            val watchedKeys = Set(watchMap.keySet.toSeq: _*)
            val removedChildren = watchedKeys -- childrenSet
            val addedChildren = childrenSet -- watchedKeys

            watchMap.synchronized {

                for (child <- removedChildren) {
                    if (log.isInfoEnabled)
                        log.info { "--- Node %s: child %s removed".format(node, child) }

                    watchMap -= child
                }

                for (child <- addedChildren) {
                    if (log.isInfoEnabled)
                        log.info { "--- Node %s: child %s added".format(node, child) }

                    watchNode("%s/%s".format(node, child), nodeChanged(child))
                }
            }

            for (child <- removedChildren) {
                notifier.map(f => f(child))
            }
        }

        watchChildren(node, parentWatcher)
    }
}

