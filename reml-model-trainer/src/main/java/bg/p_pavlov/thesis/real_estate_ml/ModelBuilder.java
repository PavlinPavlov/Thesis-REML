package bg.p_pavlov.thesis.real_estate_ml;

import ml.combust.mleap.spark.SimpleSparkSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.BATHROOMS;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.BEDROOMS;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.CONDITION;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.FLOORS;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.GRADE;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.SQUARE_FOOT_LIVING;
import static bg.p_pavlov.thesis.real_estate_ml.common.constants.RealEstateDataColumnConstants.ZIPCODE;

public class ModelBuilder {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        Dataset<Row> realEstateDF = new Loader().loadRealEstateData("local[*]", "src/main/resources/ml-data/kc_house_data.csv");

        realEstateDF = new DataCategorizer().encodeColumns(realEstateDF);

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

        realEstateDF.drop(
                GRADE,
                "grade_index",
                ZIPCODE,
                "zipcode_index",
                CONDITION,
                "condition_index"
        );

        LinearRegressionModel model = new LinearRegressionTrainer().trainLinearRegression(realEstateDF);

        Dataset<Row> withPrediction = model.transform(realEstateDF);
        new SimpleSparkSerializer().serializeToBundle(model, "jar:file:/model.zip", withPrediction);
    }
}
