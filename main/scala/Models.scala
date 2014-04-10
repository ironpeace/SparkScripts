class History(
	val customer_ID:String,
	val shopping_pt:String,
	val record_type:String,
	val day:String,
	val time:String,
	val state:String,
	val location:String,
	val group_size:Int,
	val homeowner:String,
	val car_age:Int,
	val car_value:String,
	val risk_factor:String,
	val age_oldest:Int,
	val age_youngest:Int,
	val married_couple:String,
	val c_previous:String,
	val duration_previous:Int,
	val A:String,
	val B:String,
	val C:String,
	val D:String,
	val E:String,
	val F:String,
	val G:String,
	val cost:Int
	) extends Serializable {

	def policy:String = A + B + C + D + E + F + G

}

//customer_ID,shopping_pt,record_type,day,time,state,location,group_size,homeowner,car_age,car_value,risk_factor,age_oldest,age_youngest,married_couple,c_previous,duration_previous,A,B,C,D,E,F,G,cost


class Grouped(
	val customer_ID:String,
	val day:String,
	val state:String,
	val location:String,
	val group_size:Int,
	val homeowner:String,
	val car_age:Int,
	val car_value:String,
	val risk_factor:String,
	val age_oldest:Int,
	val age_youngest:Int,
	val married_couple:String,
	val c_previous:String,
	val duration_previous:Int,

	val isSingle_state:Boolean,
	val isSingle_location:Boolean,
	val isSingle_group_size:Boolean,
	val isSingle_homeowner:Boolean,
	val isSingle_car_age:Boolean,
	val isSingle_car_value:Boolean,
	val isSingle_risk_factor:Boolean,
	val isSingle_age_oldest:Boolean,
	val isSingle_age_youngest:Boolean,
	val isSingle_married_couple:Boolean,
	val isSingle_c_previous:Boolean,
	val isSingle_duration_previous:Boolean,

	val count:Int,			// 全部で何回見積もりしたか

	val minCost:Int,
	val meanCost:Int,
	val maxCost:Int,

	val minCostPolicy:String,
	val maxCostPolicy:String,
	val evePolicy:String,
	val firstPolicy:String,
	val lastPolicy:String,

	val daysCount:Int,
	val howLong:Int,

	val howPrevPolicy:Int,
	val finCost:Int

) extends Serializable
{
	override def toString = {
		customer_ID + "," +
		day + "," +
		state + "," +
		location + "," +
		group_size + "," +
		homeowner + "," +
		car_age + "," +
		car_value + "," +
		risk_factor + "," +
		age_oldest + "," +
		age_youngest + "," +
		married_couple + "," +
		c_previous + "," +
		duration_previous + "," +
		isSingle_state + "," + 
		isSingle_location + "," + 
		isSingle_group_size + "," + 
		isSingle_homeowner + "," + 
		isSingle_car_age + "," + 
		isSingle_car_value + "," + 
		isSingle_risk_factor + "," + 
		isSingle_age_oldest + "," + 
		isSingle_age_youngest + "," + 
		isSingle_married_couple + "," + 
		isSingle_c_previous + "," + 
		isSingle_duration_previous + "," + 
		count + "," + 
		minCost + "," + 
		meanCost + "," + 
		maxCost + "," + 
		"p" + minCostPolicy + "," + 
		"p" + maxCostPolicy + "," + 
		"p" + evePolicy + "," + 
		"p" + firstPolicy + "," + 
		"p" + lastPolicy + "," +
		daysCount + "," + 
		howLong + "," + 
		howPrevPolicy + "," + 
		finCost
  	}
}


