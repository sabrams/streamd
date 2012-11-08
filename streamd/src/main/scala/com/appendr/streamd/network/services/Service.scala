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
package com.appendr.streamd.network.services

import collection.mutable

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

trait Services[T <: Service] {
    protected val map = new mutable.HashMap[String, T]
    def registerService(t: T) {
        t.commands.map(k => if (!map.contains(k)) map.put(t.name + ":" + k, t))
    }
}

trait Service {
    def name: String
    def commands: List[String]
    def command(cmd: Array[String]): String
    def commandHelp: List[String]
    def shutdown()
}
