import org.apache.spark.SparkContext

object POPI {

  def main(args: Array[String]) {

    val sc = new SparkContext(args(0), "POPI", System.getenv("SPARK_HOME"))
    var R = sc.textFile(args(1))
    var S = sc.textFile(args(2))

    var r_pairs = R.map { x => var pos = x.indexOf('|')
      (x.substring(0, pos).toLong, x.substring(pos + 1))
    }

    var s_pairs = S.map { x => var pos = x.indexOf('|')
      (x.substring(0, pos).toLong, x.substring(pos + 1))
    }

    var skew = s_pairs.sample(false, 1).map(x => (x._1, 1)).reduceByKey(_ + _).collectAsMap().filter(x => x._1 == 1)

    val broadCastSkew = sc.broadcast(skew)
    //partition S
    var s_loc = s_pairs.filter(x => broadCastSkew.value.get(x._1) != None)
    var s_dis = s_pairs.filter(x => broadCastSkew.value.get(x._1) == None)

    //partition R
    var r_dup = r_pairs.filter(x => broadCastSkew.value.get(x._1) != None)
    var r_dis = r_pairs.filter(x => broadCastSkew.value.get(x._1) == None)

    //left-outer join between redistributed part
    var results1 = r_dis.leftOuterJoin(s_dis)
    println("POPI number of result1 is " + results1.count())

    //duplication-based inner joins for r_dup and s_loc
    val broadCastMap = sc.broadcast(r_dup.collectAsMap())
    var results2 = s_loc.map(x => (x._1, broadCastMap.value.get(x._1), x._2))

    println("POPI number of result2 is " + results2.count())
    sc.stop()

  }
}

