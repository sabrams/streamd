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
package com.appendr.streamd.cluster

import com.appendr.streamd.stream.StreamEvent

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

trait Router {
    def route(node: Node, stream: StreamEvent)
    def getRoute(stream: StreamEvent): Option[Node]
    def node: Option[Node]
}
