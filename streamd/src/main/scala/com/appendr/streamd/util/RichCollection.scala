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
package com.appendr.streamd.util
import scala.collection.IterableLike
import scala.collection.generic.CanBuildFrom

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object RichCollection {
    implicit def toRich[A, Repr](xs: IterableLike[A, Repr]) = new RichCollection(xs)
}

class RichCollection[A, Repr](xs: IterableLike[A, Repr]) {
    def distinctBy[B, That](f: A => B)(implicit cbf: CanBuildFrom[Repr, A, That]) = {
        val builder = cbf(xs.repr)
        val i = xs.iterator
        var set = Set[B]()
        while(i.hasNext) {
            val o = i.next
            val b = f(o)
            if (!set(b)) {
                set += b
                builder += o
            }
        }
        builder.result
    }
}


