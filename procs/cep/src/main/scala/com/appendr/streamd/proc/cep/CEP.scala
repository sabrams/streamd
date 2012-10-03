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
package com.appendr.streamd.proc.cep

import com.appendr.streamd.stream.{StreamTuple, StreamProc}
import com.appendr.streamd.store.Store
import com.appendr.streamd.sink.Sink
import com.appendr.streamd.conf.Configuration
import esper.Engine

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class CEP extends StreamProc {
    private val engine: Engine = new Engine

    def proc(t: StreamTuple, s: Option[Store], o: Option[Sink]): Option[StreamTuple] = {
        engine.event(t._1, t._3)
        None
    }

    def coll(t: StreamTuple) {
        // no-op
    }

    def open(conf: Option[Configuration]) {
        engine.open(conf)
    }

    def close() {
        engine.close()
    }
}
