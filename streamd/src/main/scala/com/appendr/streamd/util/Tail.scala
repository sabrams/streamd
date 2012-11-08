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

import java.io.{BufferedReader, FileNotFoundException, RandomAccessFile, File}
import java.util.concurrent.atomic.AtomicBoolean
import annotation.tailrec

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object Tail {
    def apply(file: File, priorLines: Long, cb: (String) => Unit) = {
        val tail = new Tail(file, priorLines, cb)
        val thread = new Thread(tail)
        thread.setDaemon(true)
        thread.start()
        tail
    }
}

//TODO: make this more functional or rewrite it
class Tail(
    private val file: File,
    private val priorLines: Long,
    private val cb: (String) => Unit) extends Runnable {
    private val running = new AtomicBoolean(true)

    def run() { loop(init) }
    def stop { running.set(false) }

    private def createReader: Option[RandomAccessFile] = {
        try { Some(new RandomAccessFile(file, "r")) }
        catch { case e:Exception => None}
    }

    private def init: (RandomAccessFile, Long, Long) = {
        var reader: Option[RandomAccessFile] = None
        while (running.get() && reader.isEmpty) {
            reader = createReader
            if (reader.isEmpty) Thread.sleep(1000)
        }

        val spos = file.length - priorLines
        seek(reader.get, if (spos < 0) None else Some(spos))
    }

    private def seek(reader: RandomAccessFile, seekTo: Option[Long] = None) = {
        val pos = seekTo.getOrElse(file.length())
        reader.seek(pos)
        (reader, pos, System.currentTimeMillis())
    }

    private def read(reader: RandomAccessFile, pos: Long): Long = {
        reader.seek(pos)
        0
    }

    //@tailrec
    private def loop(t: (RandomAccessFile, Long, Long)) {
        /*
        if (running.get()) {
            var raf = t._1
            var pos = t._2
            val len = t._1.length()

            // file was rotated
            if (len < pos) {
                raf.close()
                raf = createReader.getOrElse(throw new FileNotFoundException)
                pos = 0
            }
            else if (len > pos || file.lastModified() > t._3) pos = read(raf, pos)
            else Thread.sleep(1000)

            loop((raf, pos, file.lastModified()))
        }*/
    }
}