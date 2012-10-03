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

import com.appendr.streamd.store.Store
import com.redis.{RedisClient, RedisClientPool}
import com.appendr.streamd.conf.Configuration

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class PooledStore(host: String, port: Int) extends Store {
    private val redis: RedisClientPool = RedisStore(host, port)
    private def client[T](body: RedisClient => T) = {
        redis.withClient {
            client => body(client)
        }
    }

    def get(key: String) = {
        client(r => r.get(key))
    }

    def get(keys: String*) = {
        client(r => r.mget(keys).get)
    }

    def set(key: String, value: Object) {
        client(r => r.set(key, value))
    }

    def add(key: String, value: (_, Object)) {
        client(r => r.lpush(key, value._1, value._2))
    }

    def rem(key: String) = {
        client {
            r => {
                val v = r.get(key)
                r.del(key)
                v
            }
        }
    }

    def rem(key: (String, String)) = {
        client {
            r => {
                val v = r.hget(key._1, key._2)
                r.del(key)
                v
            }
        }
    }

    def has(key: String) = {
        client(r => r.exists(key))
    }

    def has(key: (String, String)) = {
        client(r => r.hexists(key._1, key._2))
    }

    def inc(key: String) {
        client(r => r.incr(key))
    }

    def inc(key: (String, String)) {
        client(r => r.hincrby(key._1, key._2, 1))
    }

    def inc(key: String, i: Int) {
        client(r => r.incrby(key, i))
    }

    def inc(key: (String, String), i: Int) {
        client(r => r.hincrby(key._1, key._2, i))
    }

    def open(config: Option[Configuration]) {
        // TODO: Refactor to use trait properly
    }

    def close() {
        redis.close
    }
}
