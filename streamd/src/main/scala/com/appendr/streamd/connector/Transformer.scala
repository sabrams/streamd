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
package com.appendr.streamd.connector

import com.appendr.streamd.stream.StreamTuple

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

trait InputTransformer[I] {
    def transform(in: I): StreamTuple
}

trait OutputTransformer[O] {
    def transform(out: StreamTuple): O
}

object ByteArrayToStringInput {
    def apply(x: InputTransformer[String]) = {
        new ByteArrayToStringInput(x)
    }
}

class ByteArrayToStringInput(xfrm: InputTransformer[String]) extends InputTransformer[Array[Byte]] {
    def transform(in: Array[Byte]) = {
        xfrm.transform(new String(in))
    }
}