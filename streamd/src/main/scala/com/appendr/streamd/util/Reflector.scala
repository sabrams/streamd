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

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object Reflector {
    def apply[T <: AnyRef](className: String): T = {
        newInstance(className)(Thread.currentThread().getContextClassLoader)
    }

    def newInstance[T <: AnyRef](className: String)(implicit classLoader: ClassLoader): T = {
        val clazz: Class[T] = className
        clazz.getConstructor().newInstance()
    }

    private implicit def string2Class[T <: AnyRef]
        (name: String)(implicit classLoader: ClassLoader): Class[T] = {
        val clazz = Class.forName(name, true, classLoader)
        clazz.asInstanceOf[Class[T]]
    }

    sealed abstract class WithType {
        val clazz : Class[_]
        val value : AnyRef
    }

    case class ValWithType(anyVal: AnyVal, clazz: Class[_]) extends WithType {
        lazy val value = toAnyRef(anyVal)
    }

    case class RefWithType(anyRef: AnyRef, clazz: Class[_]) extends WithType {
        val value = anyRef
    }

    def newInstance[T <: AnyRef](className: String, args: WithType*)(implicit classLoader: ClassLoader): T  = {
        val clazz: Class[T] = className
        val argTypes = args.map(_.clazz).toArray
        val candidates = clazz.getConstructors filter { cons => matchingTypes(cons.getParameterTypes, argTypes)}
        require(candidates.length == 1, "Argument runtime types must select exactly one constructor")
        val params = args map { _.value }
        candidates.head.newInstance(params: _*).asInstanceOf[T]
    }

    private def matchingTypes(declared: Array[Class[_]], actual: Array[Class[_]]): Boolean = {
        declared.length == actual.length && (
            (declared zip actual) forall {
                case (declared, actual) => declared.isAssignableFrom(actual)
            })
    }

    implicit def refWithType[T <: AnyRef](x: T) = RefWithType(x, x.getClass)
    implicit def valWithType[T <: AnyVal](x: T) = ValWithType(x, getType(x))

    def getType(x: AnyVal): Class[_] = x match {
        case _: Byte => java.lang.Byte.TYPE
        case _: Short => java.lang.Short.TYPE
        case _: Int => java.lang.Integer.TYPE
        case _: Long => java.lang.Long.TYPE
        case _: Float => java.lang.Float.TYPE
        case _: Double => java.lang.Double.TYPE
        case _: Char => java.lang.Character.TYPE
        case _: Boolean => java.lang.Boolean.TYPE
        case _: Unit => java.lang.Void.TYPE
    }

    def toAnyRef(x: AnyVal): AnyRef = x match {
        case x: Byte => Byte.box(x)
        case x: Short => Short.box(x)
        case x: Int => Int.box(x)
        case x: Long => Long.box(x)
        case x: Float => Float.box(x)
        case x: Double => Double.box(x)
        case x: Char => Char.box(x)
        case x: Boolean => Boolean.box(x)
        case x: Unit => ()
    }
}


