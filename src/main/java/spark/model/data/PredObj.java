package spark.model.data;

/**
 * 출력데이터의 객체 클래스
 * 모델의 결과 값을 객체 형태로 만들 때 사용.
 * 이 클래스를 JSON형태로 변환하여 카프카로 출력.
 */
public class PredObj {
    private float[][] predictions;

    public float[][] getPredictions() {
        return predictions;
    }

    public void setPredictions(float[][] predictions) {
        this.predictions = predictions;
    }
}
