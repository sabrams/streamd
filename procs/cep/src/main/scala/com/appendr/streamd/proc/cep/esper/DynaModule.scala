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
package com.appendr.streamd.proc.cep.esper

import java.util.{UUID, Date}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object DynaModule {
    def apply(statement: String, subscriber: Class[_]) = {
        new DynaModule(statement, subscriber)
    }
}

class DynaModule(
    val statement: String,
    val subscriber: Class[_],
    val name: Option[String] = None,
    val schema: Option[String] = None) {
    private val mname = name.getOrElse(UUID.randomUUID().toString)
    val epl = toString

    override def toString = {
        val sb = new StringBuilder("module ")

        sb.append(mname.replace("-", ".m")).append(";\n")
        sb.append("import ").append(classOf[Subscriber].getName).append(";\n")
        sb.append("import ").append(subscriber.getName).append(";\n")

        if (schema.isDefined) {
            sb.append(schema.get)
        }

        sb.append(String.format("@Name('%s')\n", name))
        sb.append("@Description('Dynamic Esper Module')\n")
        sb.append(String.format("@Subscriber('%s')\n", subscriber.getName))
        sb.append(statement).append(";\n")
        sb.toString()
    }
}

abstract class ModuleInfo {
    val id: String
    val name: String
    val deployed: Date
    val statements: List[String]
    val isActive: Boolean
}
