package spark.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import spark.model.data.InstObj;
import spark.model.data.PredObj;

import java.io.IOException;

public class Inference {
    private SavedModelBundle savedModelBundle;
    private Session sess;

    public Inference(String modelPath) {
        this.savedModelBundle = SavedModelBundle.load(modelPath, "serve");
        this.sess = savedModelBundle.session();
    }

    public PredObj execute(InstObj instObj, String modelInput, String modelOutput) {
        PredObj predObj = new PredObj();

        Tensor x = Tensor.create(instObj.getInstances());
        Tensor result = sess.runner()
                .feed(modelInput, x)
                .fetch(modelOutput)
                .run()
                .get(0);

        predObj.setPredictions((float[][]) result.copyTo(new float[1][10]));
        return predObj;
    }
}
