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
    def get(key: String): Object
    def get(keys: String*): List[_]
    def set(key: String, value: Object)
    def add(key: String, value: (_, Object))
    def rem(key: String): Object
    def rem(key: (String, String)): Object
    def has(key: String): Boolean
    def has(key: (String, String)): Boolean
    def inc(key: String)
    def inc(key: (String,String))
    def inc(key: String, i: Int)
    def inc(key: (String, String), i: Int)
}
