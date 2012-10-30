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
package com.appendr.streamd.actors

import scalaz.concurrent.Actor
import java.util.concurrent.ConcurrentLinkedQueue

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

/**
 * Evil pimp so we can monitor mbox size
 */
object RichActor {
    class RichActor[T](a: Actor[T]) {
        val field = a.getClass.getDeclaredField("scalaz$concurrent$Actor$$mbox")
        field.setAccessible(true)
        val mbox = field.get(a).asInstanceOf[ConcurrentLinkedQueue[_]]
        field.setAccessible(false)
        def getCount(): Int = mbox.size()
    }

    implicit def actor2RichActor[T](a: Actor[T]) = new RichActor(a)
}





