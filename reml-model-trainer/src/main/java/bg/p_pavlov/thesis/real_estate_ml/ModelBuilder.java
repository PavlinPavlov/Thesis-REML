package bg.p_pavlov.thesis.real_estate_ml;

import ml.combust.mleap.spark.SimpleSparkSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.OneHotEncoderModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.BATHROOMS;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.BEDROOMS;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.FLOORS;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.SQUARE_FOOT_LIVING;

public class ModelBuilder {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);


        Dataset<Row> realEstateDF = new DataLoader().loadRealEstateData("local[*]", "src/main/resources/ml-data/kc_house_data.csv");

        StringIndexerModel gradeModel = getIndexerForColumn("grade")
                .fit(realEstateDF);

        StringIndexerModel conditionModel = getIndexerForColumn("condition")
                .fit(realEstateDF);

        StringIndexerModel zipcodeModel = getIndexerForColumn("zipcode")
                .fit(realEstateDF);


        realEstateDF = gradeModel.transform(realEstateDF);
        realEstateDF = conditionModel.transform(realEstateDF);
        realEstateDF = zipcodeModel.transform(realEstateDF);

        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCols(new String[]{"grade_index", "condition_index", "zipcode_index"})
                .setOutputCols(new String[]{"grade_vector", "condition_vector", "zipcode_vector"});

        OneHotEncoderModel oneHotEncoderModel = encoder.fit(realEstateDF);

        realEstateDF = oneHotEncoderModel.transform(realEstateDF);

        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{
                        BEDROOMS,
                        BATHROOMS,
                        SQUARE_FOOT_LIVING,
                        FLOORS,
                        "grade_vector",
                        "condition_vector",
                        "zipcode_vector"
                })
                .setOutputCol("features");

        realEstateDF = vectorAssembler.transform(realEstateDF);


        LinearRegressionModel linearRegressionModel = new LinearRegressionTrainer().trainLinearRegression(realEstateDF);

        realEstateDF = linearRegressionModel.transform(realEstateDF);


        PipelineModel plm = new PipelineModel("real-estate-pipeline-model",
                new Transformer[]{
                        gradeModel,
                        zipcodeModel,
                        conditionModel,
                        oneHotEncoderModel,
                        vectorAssembler,
                        linearRegressionModel
                });

        new SimpleSparkSerializer().serializeToBundle(plm, "jar:file:/real-estate-pipeline-model.zip", realEstateDF);

        realEstateDF.show();
    }

    private static StringIndexer getIndexerForColumn(String col) {
        return new StringIndexer()
                .setInputCol(col)
                .setOutputCol(col + "_index")
                .setHandleInvalid("keep");
    }
}
