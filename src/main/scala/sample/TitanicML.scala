package sample
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, SparkSession}

object TitanicML {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .appName("Titanic R")
      .master("local")
      .getOrCreate()

    //We'll define a partial schema with the values we are interested in. For the sake of the example points is a Double
    //We read the data from the file taking into account there's a header.
    //na.drop() will return rows where all values are non-null.
    val schemaStruct = StructType(
      StructField("PassengerId", DoubleType) ::
        StructField("Pclass", DoubleType) ::
        StructField("Parch", DoubleType) ::
        StructField("Port", DoubleType) ::
          StructField("Survived", DoubleType) ::
          StructField("isAlone", DoubleType) ::
          StructField("hasCabin", DoubleType) ::
          StructField("group_fare", DoubleType) ::
          StructField("group_family", DoubleType) ::
          StructField("Cabin_class", DoubleType) ::
          StructField("Sex_group", DoubleType) ::
          StructField("age_group", DoubleType) ::
          StructField("group_title", DoubleType) ::Nil
    )
    val df = spark.read
      .option("header", true).schema(schemaStruct)
      .csv("data/preprocessed")
      .na.drop()
//    val df_ = df.drop("PassengerId")
    println(df.count)
    //We'll split the set into training and test data
    val Array(trainingData, testData) = df.randomSplit(Array(0.8, 0.2))

    val labelColumn = "Survived"

    //We define two StringIndexers for the categorical variables

    //We define the assembler to collect the columns into a new column with a single vector - "features"
    val assembler = new VectorAssembler()
      .setInputCols(Array("Pclass", "Parch", "Port", "isAlone", "hasCabin", "group_fare", "group_family", "Cabin_class", "Sex_group", "age_group", "group_title"))
      .setOutputCol("features")

    //For the regression we'll use the Gradient-boosted tree estimator
    val gbt = new GBTRegressor()
      .setLabelCol(labelColumn)
      .setFeaturesCol("features")
      .setPredictionCol("Predicted " + labelColumn)
      .setMaxIter(50)

    //We define the Array with the stages of the pipeline
    val stages = Array(
      assembler,
      gbt
    )

    //Construct the pipeline
    val pipeline = new Pipeline().setStages(stages)

    //We fit our DataFrame into the pipeline to generate a model
    val model = pipeline.fit(trainingData)

    //We'll make predictions using the model and the test data
    val predictions = model.transform(testData)
    predictions.show

    val roundUDF = udf((x: Double) => Math.round(x))
    val pred = predictions.withColumn("pred", roundUDF(col("Predicted Survived")))
    val compareUDF = udf((x: Double, y: Double) => if (x == y) {1} else {0})
    val pred_ = pred.withColumn("compare", compareUDF(col("Survived"), col("pred")))
    val count_right = pred_.groupBy("compare").count
    count_right.show
    //This will evaluate the error/deviation of the regression using the Root Mean Squared deviation
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelColumn)
      .setPredictionCol("Predicted " + labelColumn)
      .setMetricName("rmse")

    //We compute the error using the evaluator
    val error = evaluator.evaluate(predictions)

    println(error)

    spark.stop()
  }
}
