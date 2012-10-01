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

import com.appendr.streamd.conf.ConfigSpec

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class ZKConfigSpec extends ConfigSpec[ZKClient] {
    var hosts = required[String]
    var timeout = required[Int]
    var path = required[String]

    def apply() = {
        new ZKClient(this)
    }
}