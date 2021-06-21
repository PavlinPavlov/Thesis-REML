package bg.p_pavlov.thesis.real_estate_ml;

import ml.combust.mleap.spark.SimpleSparkSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ModelBuilder {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

// ######################### Load Data #########################################

        Dataset<Row> realEstateDF = new Loader().loadRealEstateData("local[*]", "src/main/resources/ml-data/kc_house_data.csv");

// ######################### Vectorize categorical data #########################################

        StringIndexer gradeIndexer = getIndexerForColumn("grade");
        realEstateDF = gradeIndexer
                .fit(realEstateDF)
                .transform(realEstateDF);

        StringIndexer conditionIndexer = getIndexerForColumn("condition");
        realEstateDF = conditionIndexer
                .fit(realEstateDF)
                .transform(realEstateDF);

        StringIndexer zipcodeIndexer = getIndexerForColumn("zipcode");
        realEstateDF = zipcodeIndexer
                .fit(realEstateDF)
                .transform(realEstateDF);

        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCols(new String[]{"grade_index", "condition_index", "zipcode_index"})
                .setOutputCols(new String[]{"grade_vector", "condition_vector", "zipcode_vector"});

        realEstateDF = encoder.fit(realEstateDF).transform(realEstateDF);

// ######################### Create features vector #########################################

        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{"bedrooms", "bathrooms", "sqft_living", "floors", "grade_vector", "condition_vector", "zipcode_vector"})
                .setOutputCol("features");

        realEstateDF = vectorAssembler.transform(realEstateDF);

        realEstateDF.drop("grade", "grade_index", "zipcode", "zipcode_index", "condition", "condition_index");

// ######################### Train #########################################

        LinearRegressionModel model = train(realEstateDF);
        Dataset<Row> withPrediction = model.transform(realEstateDF);

// ######################### Export #########################################

        new SimpleSparkSerializer().serializeToBundle(model, "jar:file:/model.zip", realEstateDF);

    }

    private static LinearRegressionModel train(Dataset<Row> dataframe) {

        Dataset<Row>[] split = dataframe.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainDF = split[0];
        Dataset<Row> holdout = split[1];

        LinearRegression linearRegression = new LinearRegression();

        ParamMap[] paramMaps = new ParamGridBuilder()
                .addGrid(linearRegression.regParam(), new double[]{0.001, 0.005, 0.01, 0.05, 0.1, 0.3, 0.5, 0.8, 1})
                .addGrid(linearRegression.elasticNetParam(), new double[]{0, 0.01, 0.25, 0.33, 0.5, 0.66, 0.75, 1})
                .build();

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                .setEstimator(linearRegression)
                .setEvaluator(new RegressionEvaluator().setMetricName("r2")) // what to maximise
                .setEstimatorParamMaps(paramMaps)
                .setTrainRatio(0.8);


        TrainValidationSplitModel trainValidationSplitModel = trainValidationSplit.fit(trainDF);

        LinearRegressionModel model = (LinearRegressionModel) trainValidationSplitModel.bestModel();

        System.out.println("Hold out data: " + model.evaluate(holdout).r2());
        System.out.println("Hold out data: " + model.evaluate(holdout).rootMeanSquaredError());

        return model;
    }

    private static StringIndexer getIndexerForColumn(String col) {
        return new StringIndexer()
                .setInputCol(col)
                .setOutputCol(col + "_index");
    }
}
