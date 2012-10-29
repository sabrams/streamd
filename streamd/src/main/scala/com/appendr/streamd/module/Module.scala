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
package com.appendr.streamd.module

import com.appendr.streamd.stream.StreamProc
import com.appendr.streamd.sink.Sink
import com.appendr.streamd.store.Store
import com.appendr.streamd.controlport.ControlPortHandler
import com.appendr.streamd.util.LifeCycle
import com.appendr.streamd.conf.Configuration

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

trait Module extends LifeCycle {
    def id(): String = getClass.getName
    def proc(): Option[StreamProc]
    def sink(): Option[Sink]
    def store(): Option[Store]
    def cport(): Option[ControlPortHandler]
    def open() {
        if (proc().isDefined) proc().get.open()
        if (sink().isDefined) sink().get.open()
        if (store().isDefined) store().get.open()
    }
    def close() {
        if (proc().isDefined) proc().get.close()
        if (sink().isDefined) sink().get.close()
        if (store().isDefined) store().get.close()
    }
}

trait ModuleContext {
    var moduleContext: (Option[Sink], Option[Store]) = (None, None)
}

class ModuleBuilder {

}
