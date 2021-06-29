package bg.p_pavlov.thesis.real_estate_ml;

import ml.combust.mleap.spark.SimpleSparkSerializer;
import org.apache.spark.ml.Transformer;

public class ModelLoader {

    public Transformer load() {
        return new SimpleSparkSerializer().deserializeFromBundle("jar:file:/real-estate-pipeline-model.zip");
    }
}
