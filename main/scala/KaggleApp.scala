import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


object KaggleApp { 
	def main(args: Array[String]) { 

		val sc = new SparkContext("local", "Kaggle")
		
		println(" ************************* train ")
		impl(sc.textFile("mydata/train.csv"), "train")

		println(" ************************* test ")
		impl(sc.textFile("mydata/test.csv"), "test")

	}

	def impl(tf:RDD[String], typ:String) {

		// 読み込んだCSVファイルの件数を確認
		println("train length : " + tf.count())
		// [info] train length : 665249

		// CSVを１行ずつHistoryオブジェクトに格納
		val histories:RDD[History] 
			= tf.map(line => line.split(",")).map(cols => 
				new History(
					cols(0), 			// customer_ID
					cols(1),			// shopping_pt
					cols(2),			// record_type
					cols(3),			// day
					cols(4),			// time
					cols(5),			// state
					cols(6),			// location
					cols(7).toInt,		// group_size
					cols(8),			// homeowner
					cols(9).toInt,		// car_age
					cols(10),			// car_value
					cols(11),			// risk_factor
					cols(12).toInt,		// age_oldest
					cols(13).toInt,		// age_youngest
					cols(14),			// married_couple
					cols(15),			// C_previous

					// NAだったら0を挿入する
					if(cols(16) == "NA") 0 else cols(16).toInt,		// duration_previous

					cols(17),			// A
					cols(18),			// B
					cols(19),			// C
					cols(20),			// D
					cols(21),			// E
					cols(22),			// F
					cols(23),			// G
					cols(24).toInt		// cost
				)
			)

		// customer_ID をキーにグルーピングした結果を、 Grouped オブジェクトに格納

		if(typ == "train"){
			val groupedTrain:RDD[Grouped]
				= histories
					.map(hist => (hist.customer_ID, hist))
					.groupByKey().mapValues { hists =>
						new Grouped(
							hists.last.customer_ID,
							hists.last.day,
							hists.last.state,
							hists.last.location,
							hists.last.group_size,
							hists.last.homeowner,
							hists.last.car_age,
							hists.last.car_value,
							hists.last.risk_factor,
							hists.last.age_oldest,
							hists.last.age_youngest,
							hists.last.married_couple,
							hists.last.c_previous,
							hists.last.duration_previous,

							// customer_ID キーでグルーピングされたリストに対して、
							// さらに 各プロパティ	 でグルーピングしてその件数を確認
							// １件だけならグルーピングの中で唯一と判断できる
							if(hists.groupBy(_.state).size == 1) true else false,
							if(hists.groupBy(_.location).size == 1) true else false,
							if(hists.groupBy(_.group_size).size == 1) true else false,
							if(hists.groupBy(_.homeowner).size == 1) true else false,
							if(hists.groupBy(_.car_age).size == 1) true else false,
							if(hists.groupBy(_.car_value).size == 1) true else false,
							if(hists.groupBy(_.risk_factor).size == 1) true else false,
							if(hists.groupBy(_.age_oldest).size == 1) true else false,
							if(hists.groupBy(_.age_youngest).size == 1) true else false,
							if(hists.groupBy(_.married_couple).size == 1) true else false,
							if(hists.groupBy(_.c_previous).size == 1) true else false,
							if(hists.groupBy(_.duration_previous).size == 1) true else false,

							hists.filter(_.record_type == "0").size,										// count

							hists.filter(_.record_type == "0").minBy(_.cost).cost,											// minCost
							hists.filter(_.record_type == "0").map(_.cost).sum / hists.filter(_.record_type == "0").size,	// meanCost
							hists.filter(_.record_type == "0").maxBy(_.cost).cost,											// maxCost

							hists.minBy(_.cost).policy,						// minCostPolicy
							hists.maxBy(_.cost).policy,						// maxCostPolicy
							hists.last.policy, 								// evePolicy
							hists.filter(_.shopping_pt == "1")(0).policy, 	// firstPolicy
							hists.filter(_.record_type == "1")(0).policy,	// lastPolicy

							hists.filter(_.record_type == "0").groupBy(_.day).size,	// daysCount
							howLong(hists.filter(_.record_type == "0")),			// howLong

							howPrev(hists),											// howPrevPolicy
							hists.filter(_.record_type == "1")(0).cost				// finCost

						)
					}.map(groupedHistory => groupedHistory._2)

			// グループングされた件数を確認
			println("groupedTrain length :" + groupedTrain.count())
			groupedTrain.saveAsTextFile("mydata/groups.train")

		}else if(typ == "test"){
			val groupedTest:RDD[Grouped]
				= histories
					.map(hist => (hist.customer_ID, hist))
					.groupByKey().mapValues { hists =>
						new Grouped(
								hists.last.customer_ID,
								hists.last.day,
								hists.last.state,
								hists.last.location,
								hists.last.group_size,
								hists.last.homeowner,
								hists.last.car_age,
								hists.last.car_value,
								hists.last.risk_factor,
								hists.last.age_oldest,
								hists.last.age_youngest,
								hists.last.married_couple,
								hists.last.c_previous,
								hists.last.duration_previous,

								// customer_ID キーでグルーピングされたリストに対して、
								// さらに 各プロパティ	 でグルーピングしてその件数を確認
								// １件だけならグルーピングの中で唯一と判断できる
								if(hists.groupBy(_.state).size == 1) true else false,
								if(hists.groupBy(_.location).size == 1) true else false,
								if(hists.groupBy(_.group_size).size == 1) true else false,
								if(hists.groupBy(_.homeowner).size == 1) true else false,
								if(hists.groupBy(_.car_age).size == 1) true else false,
								if(hists.groupBy(_.car_value).size == 1) true else false,
								if(hists.groupBy(_.risk_factor).size == 1) true else false,
								if(hists.groupBy(_.age_oldest).size == 1) true else false,
								if(hists.groupBy(_.age_youngest).size == 1) true else false,
								if(hists.groupBy(_.married_couple).size == 1) true else false,
								if(hists.groupBy(_.c_previous).size == 1) true else false,
								if(hists.groupBy(_.duration_previous).size == 1) true else false,

								hists.filter(_.record_type == "0").size,										// count

								hists.filter(_.record_type == "0").minBy(_.cost).cost,											// minCost
								hists.filter(_.record_type == "0").map(_.cost).sum / hists.filter(_.record_type == "0").size,	// meanCost
								hists.filter(_.record_type == "0").maxBy(_.cost).cost,											// maxCost

								hists.minBy(_.cost).policy,						// minCostPolicy
								hists.maxBy(_.cost).policy,						// maxCostPolicy
								hists.last.policy, 								// evePolicy
								hists.filter(_.shopping_pt == "1")(0).policy, 	// firstPolicy
								"",	// lastPolicy

								hists.filter(_.record_type == "0").groupBy(_.day).size,	// daysCount
								howLong(hists.filter(_.record_type == "0")),			// howLong

								0,													// howPrevPolicy
								0													// finCost
						)
					}.map(groupedHistory => groupedHistory._2)

			println("groupedTrain length :" + groupedTest.count())
			groupedTest.saveAsTextFile("mydata/groups.test")
		}

		/**
		この結果、mydata/groups/part-0000* というファイルが複数作成されるので、それを以下のようにひとつのファイルに結合
		touch groups.csv
		cat groups/part-00000 >> groups.csv 
		cat groups/part-00001 >> groups.csv
		cat groups.csv | wc -l
		**/

	}//*
  
  def howPrev(hists:Seq[History]):Int = {
  	val sorted = hists.sortBy(_.shopping_pt).reverse //降順ソート
	val fin = sorted(0)
	val prevs = sorted.tail
	var cnt = 0

	prevs.foreach(prev =>
		if(prev.policy == fin.policy){
			cnt = cnt + 1
			return cnt
		}else{
			cnt = cnt + 1
		}
	)
	return cnt
  }

  def howLong(hists:Seq[History]):Int = {
  	val sorted = hists.sortBy(_.shopping_pt).reverse //降順ソート

  	if(sorted.size == 1){
  		return 0
  	}else{
		// tpl : (preDay, preTime, hl)
	  	sorted.foldLeft((0, 0, 0)){ (tpl, hist) => 
	  		val nowTime =  toMin(hist.time)
	  		if(tpl._1 == hist.day.toInt){
	  			(hist.day.toInt, nowTime, nowTime - tpl._2 + tpl._3)
			}else{
				(hist.day.toInt, nowTime, nowTime + tpl._3)
			}
	  	}._3
	}
  }

  def toMin(hhmm:String):Int = {
  	val h = hhmm.substring(0,2).toInt
  	val m = hhmm.substring(3,5).toInt
  	return h * 60 + m
  }

}


