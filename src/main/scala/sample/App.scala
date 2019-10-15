package sample
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, DoubleType}
import org.apache.log4j.{Level, Logger}
object App {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.master", "local").getOrCreate()
    import spark.implicits._
//    val df = spark.read.option("header", true).csv("/home/hoanlk/Desktop/train.csv")
//    val df = spark.read.option("multiLine", true).json("/home/hoanlk/Downloads/employee-data/employees_multiLine.json")
    val df = spark.read.option("header", true).csv("data/train.csv")
//    df.show()

    //check ID unique
//    val num_record = df.count
//    println(num_record, num_record.getClass())
//    val df_count_unique = df.select("PassengerId").distinct().count
//    println(df_count_unique, df_count_unique.getClass())
//    println(num_record == df_count_unique)
    val df_null_record = df.filter($"Age".isNull || $"Age".isNaN || $"Age" === "")
    val meanAge = df.filter($"Age".isNotNull).select(mean("Age")).first.get(0)
    val ageAVG = Math.round(meanAge.asInstanceOf[Double]).toString()
    val df_fillage  = df.na.fill(ageAVG, Seq("Age"))
    df_fillage.show()
    val df_casted_Survived = df_fillage.withColumn("Survived2", df("Survived").cast(IntegerType)).drop("Survived").withColumnRenamed("Survived2", "Survived")
    // statistics
    //by Parch
    val df_parch = df_casted_Survived.select("Parch", "Survived").groupBy("Parch").mean("Survived")
    df_parch.show

    //by Sex
    val df_sex = df_casted_Survived.select("Sex", "Survived").groupBy("Sex").mean("Survived")
    df_sex.show

    df_casted_Survived.select("Pclass", "Survived").groupBy("Pclass").mean().show
    df_casted_Survived.select("SibSp", "Survived").groupBy("SibSp").mean().show

    //fill Embarked
    val translationMap = Map("S" -> "0","C" -> "1","Q" -> "2")
    val getTranslationValue = udf ((x: String)=>translationMap.getOrElse(x,null.asInstanceOf[String]) )


    val df_fill_embarked = df_casted_Survived.na.fill("S", Seq("Embarked")).withColumn("Embarked", getTranslationValue(col("Embarked"))).withColumnRenamed("Embarked", "Port")

    df_fill_embarked.show
    val meanFare = df_fill_embarked.filter($"Fare".isNotNull).withColumn("Fare2", df("Fare").cast(DoubleType)).select(mean("Fare2")).first.get(0)
    println(meanFare, meanFare.getClass())
    val meanFareString = meanFare.toString()
    println(meanFareString, meanFareString.getClass())
    val df_filled_fare = df_fill_embarked.na.fill(meanFareString, Seq("Fare"))
    val df_filled_fare_ = df_filled_fare.withColumn("Fare2", df_filled_fare("Fare").cast(DoubleType)).drop("Fare").withColumnRenamed("Fare2", "Fare")
//    df_filled_fare_.show
//    df_filled_fare_.filter($"Fare".isNull).show

    //check is alone
    val familyUDF = udf((parch: Int, SibSp: Int) => parch + SibSp + 1)
    val df_family = df_filled_fare_.withColumn("FamilySize", familyUDF(col("Parch"), col("SibSp")))
//    df_family.show
    val AloneUDF = udf((familySize: Int) => if (familySize > 1) {0} else {1})
    val df_alone = df_family.withColumn("isAlone", AloneUDF(col("familySize")))
//    df_alone.show

    //has cabin
    val rplString = "HOANLK"
    val df_fill_cabin = df_alone.na.fill(rplString, Seq("Cabin"))
    val cabinUDF = udf((cabin: String) => if (cabin == "HOANLK") {0} else {1})
    val df_cabin = df_fill_cabin.withColumn("hasCabin", cabinUDF(col("Cabin")))
//    df_cabin.show

    //get title from Name
    def preprocessString(str: String): String ={
        val re = " ([A-Za-z]+)\\.".r
        val title = re.findFirstIn(str).toString()
        val new_title = title.replace("Mlle", "Miss").replace("Ms", "Miss").replace("Mme", "Mrs").replace("Some(", "").replace(")", "").trim()
//        val list_ = Seq("Lady", "Countess","Capt", "Col","Don", "Dr", "Major", "Rev", "Sir", "Jonkheer", "Dona")
//        for (i <- list_) {
//            new_title.replace(i, "Rare")
//        }
        val newest_title = new_title.replace("Lady", "Rare").replace("Capt", "Rare").replace("Countess", "Rare")
  .replace("Col", "Rare").replace("Don", "Rare").replace("Dr", "Rare").replace("Major", "Rare")
  .replace("Rev", "Rare").replace("Sir", "Rare").replace("Jonkheer", "Rare")
  .replace("Dona", "Rare")

        return newest_title
    }
    val nameUDF = udf((name: String) => {
        preprocessString(name)
    })
      val df_title = df_cabin.withColumn("title", nameUDF(col("Name")))
//      df_title.show

      //group
      val groupFareUDF = udf((Fare: Double) =>
        if (Fare < 7.91 ) {0}
          else if (Fare <= 14.454) {1}
          else if (Fare <= 31) {2}
          else {3}
      )

      val groupFamilySizeUDF = udf((familySize: Int) =>
        if (familySize > 5) {"Big"}
          else if (familySize > 1) {"Small"}
          else {"Alone"}
      )
      val groupAgeUDF = udf((Age: Int) =>
          if (Age <= 14 ) {0}
          else if (Age <= 32) {1}
          else if (Age <= 48) {2}
          else if (Age <= 64) {3}
          else {4}
      )
      val familyMap = Map("Big" -> "2", "Small" -> "1", "Alone" -> "0")
      val df_group_fare = df_title.withColumn("group_fare", groupFareUDF(col("Fare")))
      val df_group_family = df_group_fare.withColumn("group_family", groupFamilySizeUDF(col("familySize")))
      val getTranslationValue2 = udf ((x: String)=>familyMap.getOrElse(x,null.asInstanceOf[String]) )


      val df_family_group = df_group_family.withColumn("group_family", getTranslationValue2(col("group_family")))
//      df_family_group.show

      val df_fare = df_family_group.withColumn("group_fare", groupFareUDF(col("Fare")))
      val df_droped = df_fare.drop("Name", "familySize", "SibSp")
//      df_droped.show

//      df_droped.na.fill("X", Seq("Cabin"))
      val cabinUDF_ = udf((x: String) => x(0).toString())
      val cabinMap = Map(
        "A" -> "2",
        "D" -> "2",
        "E" -> "2",
        "T" -> "2",
        "B" -> "3",
        "C" -> "3",
        "F" -> "1",
        "G" -> "1",
        "H" -> "0"
      )
      val df_cabin_ = df_droped.withColumn("Cabin_First", cabinUDF_(col("Cabin")))
      val getTranslationValueCabin = udf((x: String) => cabinMap.getOrElse(x, null.asInstanceOf[String]))
      val df_map_column = df_cabin_.withColumn("Cabin_class", getTranslationValueCabin(col("Cabin_First")))
//      df_map_column.show

      val sexMap = Map(
          "male" -> "0",
          "female" -> "1"
      )
      val sexTranslate = udf((x: String) => sexMap.getOrElse(x, null.asInstanceOf[String]))
      val df_map_sex = df_map_column.withColumn("Sex_group", sexTranslate(col("Sex")))

      val df_group_age = df_map_sex.withColumn("age_group", groupAgeUDF(col("Age")))

      val mapTitle = Map(
          "Mr." -> "1",
          "Miss." -> "2",
          "Mrs." -> "3",
          "Master." -> "4",
          "Rare." -> "5"
      )

      val titleTranslateValue = udf((x: String) => mapTitle.getOrElse(x, null.asInstanceOf[String]))
      val df_map_title = df_group_age.withColumn("group_title", titleTranslateValue(col("title")))

//      df_map_title.show
      df_map_title.printSchema()
      println(df_map_title.select("title").first())
      df_map_title.filter(df_map_title("title") === "Rare.")
      val df_final = df_map_title.drop("Sex", "Ticket", "Age", "Fare", "Cabin", "title", "Cabin_First")
      df_final.show
      df_final.repartition(1).write.option("header", true).csv("data/preprocessed")
  }
}
