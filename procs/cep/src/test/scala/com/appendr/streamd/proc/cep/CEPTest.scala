package com.appendr.streamd.proc.cep

import esper.DynaModule
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.appendr.streamd.connector.InputTransformer
import com.appendr.streamd.stream.StreamTuple
import io.Source
import com.appendr.streamd.conf

@RunWith(classOf[JUnitRunner])
class CEPTest extends FunSuite with BeforeAndAfter {
    private val proc = new CEP
    private val ixf = new InputTransformer[String] {
        def transform(in: String): StreamTuple = {
            val v: Array[String] = in.split(",")
            val score: java.lang.Float = v(2).toFloat/10
            StreamTuple("colors", "color", Map[String, Object]("user"->v(0), "color"->v(1), "score"->score))
        }
    }

    before {
        proc.open(Some(conf.Configuration.fromResource("test.conf")))
        /*
        val m: DynaModule = new DynaModule(
            "select distinct color, count(color), avg(score) from colors.win:time_length_batch(10, 100);",
            classOf[TestSubscriber], Some("test-module"), Some("create map schema colors as (user String, color String, score float);"))
        proc.engine.activate(proc.engine.load(m.epl))*/
    }

    after {
        proc.close()
    }

    test("engine evaluates events") {
        val dataStream = getClass.getClassLoader.getResourceAsStream("data.csv")
        val iter = Source.fromInputStream(dataStream).getLines()

        for (s <- iter) proc.proc(ixf.transform(s), None, None)
    }
}

class TestSubscriber {
    def update(color: String, count: Long, score: Double) {
        System.out.println("-----> " + count + " " + color + " has an average score of " + score)
    }
}