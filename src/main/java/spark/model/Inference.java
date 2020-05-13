package spark.model;

import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

public class Inference {
    private SavedModelBundle savedModelBundle;
    private Session sess;

    public Inference(String modelPath) {
        this.savedModelBundle = SavedModelBundle.load(modelPath, "serve");
        this.sess = savedModelBundle.session();
    }

    public float[][] execute(float[][][][] data, String modelInput, String modelOutput) {
        Tensor x = Tensor.create(data);
        Tensor result = sess.runner()
                .feed(modelInput, x)
                .fetch(modelOutput)
                .run()
                .get(0);

        return (float[][]) result.copyTo(new float[1][10]);
    }
}
