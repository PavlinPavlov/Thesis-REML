package bg.p_pavlov.thesis.real_estate_ml;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class LinearRegressionTrainer {

    public LinearRegressionModel trainLinearRegression(Dataset<Row> dataframe) {

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
}
