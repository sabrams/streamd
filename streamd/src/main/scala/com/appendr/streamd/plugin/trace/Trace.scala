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
package com.appendr.streamd.plugin.trace

import com.appendr.streamd.stream.StreamProc
import com.appendr.streamd.plugin.PluginContextAware
import com.appendr.streamd.stream.StreamTuple
import com.appendr.streamd.conf.Configuration

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class Trace extends StreamProc with PluginContextAware {
    def proc(t: StreamTuple): Option[StreamTuple] = {
        System.out.println(t._3)
        None
    }

    def coll(t: StreamTuple) {
        // no-op
    }

    def open(conf: Option[Configuration]) {
    }

    def close() {
    }
}

class NoOp extends StreamProc with PluginContextAware {
    def proc(t: StreamTuple): Option[StreamTuple] = None
    def coll(t: StreamTuple){}
    def open(conf: Option[Configuration]){}
    def close(){}
}