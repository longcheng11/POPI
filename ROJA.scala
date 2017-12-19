import org.apache.spark.SparkContext

object ROJA {

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "ROJA", System.getenv("SPARK_HOME"))
    var R = sc.textFile(args(1))
    var S = sc.textFile(args(2))

    var r_pairs = R.map { x => var pos = x.indexOf('|')
      (x.substring(0, pos).toLong, x.substring(pos + 1))
    }

    var s_pairs = S.map { x => var pos = x.indexOf('|')
      (x.substring(0, pos).toLong, x.substring(pos + 1))
    }

    var join = r_pairs.leftOuterJoin(s_pairs)
    println("Hash number of result is " + join.count())

    sc.stop()
  }

}
