import org.apache.spark.SparkContext
import scala.collection.immutable._
import scala.collection.mutable.ListBuffer

object DER {

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "DER", System.getenv("SPARK_HOME"))
    var R = sc.textFile(args(1))
    var S = sc.textFile(args(2))

    // R is smaller, so broadcast it as a map<String, String>
    var r_pairs = R.map { x => var pos = x.indexOf('|')
      (x.substring(0, pos).toLong, x.substring(pos + 1))
    }

    var s_pairs = S.map { x => var pos = x.indexOf('|')
      (x.substring(0, pos).toLong, x.substring(pos + 1))
    }

    //DER implementation for the rest part
    val broadCastR = sc.broadcast(r_pairs.collect())

    val N = s_pairs.partitions.size //number of partitions for final id checking

    // S inner joins with R in map side, R access S actually
    val join_1 = s_pairs.mapPartitions({ iter =>
      //building the hashtable for S
      var table = new HashMap[Long, ListBuffer[String]]
      var results_1 = new ListBuffer[(Long, ListBuffer[String])]
      var m = broadCastR.value
      while (iter.hasNext) {
        var record = iter.next
        var value = table.getOrElse(record._1, null)
        if (value == null) {
          table += record._1 -> (new ListBuffer[String] += record._2)
        }
        else {
          value += record._2
        }
      }

      //hash join and also record the non-matched ones
      var i: Long = 0
      for ((key, value) <- m.iterator)
        yield {
          i += 1
          var value1 = table.getOrElse(key, null)
          if (value1 == null)
            (i, None) //non-matched
          else
            (key, (value, value1)) //matched
        }
    })

    //the matched results
    var matched = join_1.filter(x => x._2 != None)
    println("DER number of result1 is " + matched.count())

    //the keys of the non-matched part, and get the keys with number == N
    var non_keys = join_1.filter(x => x._2 == None).map(tuple => (tuple._1, 1)).reduceByKey(_ + _).filter(key => key._2 == N) //(k1)

    //the keys search the broadcast R
    var non_matched = non_keys.mapPartitions({ iter =>
      var m = broadCastR.value
      for (pos <- iter) yield {
        var pair = m.apply(pos._1.toInt - 1)
        (pair._1, pair._2, None)
      }
    })
    println("DER number of result2 is " + non_matched.count())

    sc.stop()
  }
}
