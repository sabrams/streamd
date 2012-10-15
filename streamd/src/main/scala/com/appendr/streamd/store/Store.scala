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
package com.appendr.streamd.store

import com.appendr.streamd.conf.ConfigurableResource

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

trait Store extends ConfigurableResource {
    def get(key: String): Option[_]
    def get(key: (String, String)): Option[_]
    def get(keys: String*): List[Option[_]]
    def set(key: String, value: Any)
    def set(key: (String, String), value: Any)
    def add(key: String, value: (_, Any))
    def rem(key: String): Option[_]
    def rem(key: (String, String)): Option[_]
    def has(key: String): Boolean
    def has(key: (String, String)): Boolean
    def inc(key: String)
    def inc(key: (String,String))
    def inc(key: String, i: Int)
    def inc(key: (String, String), i: Int)
}
