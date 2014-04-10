import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


object KaggleApp2 { 
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

		if(typ == "train"){
			val groupedTrain:RDD[History]
				= histories
					.map(hist => (hist.customer_ID, hist))
					.groupByKey().mapValues { hists =>
						new History(
							hists.filter(_.record_type == "1")(0).customer_ID,
							hists.filter(_.record_type == "1")(0).shopping_pt,
							hists.filter(_.record_type == "1")(0).record_type,
							hists.filter(_.record_type == "1")(0).day,
							hists.filter(_.record_type == "1")(0).time,
							hists.filter(_.record_type == "1")(0).state,
							hists.filter(_.record_type == "1")(0).location,
							hists.filter(_.record_type == "1")(0).group_size,
							hists.filter(_.record_type == "1")(0).homeowner,
							hists.filter(_.record_type == "1")(0).car_age,
							hists.filter(_.record_type == "1")(0).car_value,
							hists.filter(_.record_type == "1")(0).risk_factor,
							hists.filter(_.record_type == "1")(0).age_oldest,
							hists.filter(_.record_type == "1")(0).age_youngest,
							hists.filter(_.record_type == "1")(0).married_couple,
							hists.filter(_.record_type == "1")(0).c_previous,
							hists.filter(_.record_type == "1")(0).duration_previous,
							hists.filter(_.record_type == "1")(0).A,
							hists.filter(_.record_type == "1")(0).B,
							hists.filter(_.record_type == "1")(0).C,
							hists.filter(_.record_type == "1")(0).D,
							hists.filter(_.record_type == "1")(0).E,
							hists.filter(_.record_type == "1")(0).F,
							hists.filter(_.record_type == "1")(0).G,
							hists.filter(_.record_type == "1")(0).cost

						)
					}.map(groupedHistory => groupedHistory._2)

			// グループングされた件数を確認
			println("groupedTrain length :" + groupedTrain.count())
			groupedTrain.saveAsTextFile("mydata/groups2.train")

		}else if(typ == "test"){
			val groupedTest:RDD[History]
				= histories
					.map(hist => (hist.customer_ID, hist))
					.groupByKey().mapValues { hists =>
						new History(
								hists.last.customer_ID,
								hists.last.shopping_pt,
								hists.last.record_type,
								hists.last.day,
								hists.last.time,
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
								hists.last.A,
								hists.last.B,
								hists.last.C,
								hists.last.D,
								hists.last.E,
								hists.last.F,
								hists.last.G,
								hists.last.cost
						)
					}.map(groupedHistory => groupedHistory._2)

			println("groupedTrain length :" + groupedTest.count())
			groupedTest.saveAsTextFile("mydata/groups2.test")
		}

		/**
		この結果、mydata/groups/part-0000* というファイルが複数作成されるので、それを以下のようにひとつのファイルに結合
		touch groups.csv
		cat groups/part-00000 >> groups.csv 
		cat groups/part-00001 >> groups.csv
		cat groups.csv | wc -l
		**/

	}//*

}


