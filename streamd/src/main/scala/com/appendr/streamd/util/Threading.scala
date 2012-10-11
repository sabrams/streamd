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
import scalaz.concurrent.{Strategy, StrategyLow}
import java.util.concurrent.{ThreadFactory, ExecutorService}
import java.util.concurrent.Executors._
import jsr166y.ForkJoinPool

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

package threading {

    object ForkJoinStrategy extends StrategyLow {
        /**
         * The default executor service is a fixed thread pool with N daemon threads,
         * where N is equal to the number of available processors x 2.
         */
        lazy val forkJoinExecutorService: ExecutorService =
            new ForkJoinPool
            (
                Runtime.getRuntime.availableProcessors * 2,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                new Thread.UncaughtExceptionHandler {
                    def uncaughtException(t: Thread, e: Throwable) {
                        System.err.println(String.format("uncaughtException in thread '%s'", t))
                    }
                }, true
            )

        implicit lazy val strategy: Strategy = Executor(forkJoinExecutorService)
    }

    object FixedThreadPoolStrategy extends StrategyLow {
        /**
         * The default executor service is a fixed thread pool with N daemon threads,
         * where N is equal to the number of available processors x 2.
         */
        lazy val executorService: ExecutorService = {
            newFixedThreadPool(Runtime.getRuntime.availableProcessors * 2, new ThreadFactory {
                def newThread(r: Runnable) = {
                    val t = defaultThreadFactory.newThread(r)
                    t.setDaemon(true)
                    t
                }
            })
        }

        implicit lazy val strategy: Strategy = Executor(executorService)
    }

    object CachedThreadPoolStrategy extends StrategyLow {
        /**
         * The default executor service is a fixed thread pool with N daemon threads,
         * where N is equal to the number of available processors x 2.
         */
        lazy val executorService: ExecutorService = {
            newCachedThreadPool(new ThreadFactory {
                def newThread(r: Runnable) = {
                    val t = defaultThreadFactory.newThread(r)
                    t.setDaemon(true)
                    t
                }
            })
        }

        implicit lazy val strategy: Strategy = Executor(executorService)
    }
}
