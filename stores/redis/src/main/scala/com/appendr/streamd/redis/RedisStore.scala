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
package com.appendr.streamd.redis

import com.redis.cluster.{RegexKeyTag, RedisCluster}
import com.redis.{RedisClientPool, RedisClient}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object RedisStore {
    def apply(hosts: String*): RedisClient = {
        val c = new RedisCluster(hosts.head) {
            val keyTag = Some(RegexKeyTag)
        }

        hosts.tail.foreach(s => c.addServer(s))

        c
    }

    def apply(host: String, port: Int): RedisClientPool = {
        new RedisClientPool(host, port)
    }
}

