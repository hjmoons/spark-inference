package spark.model.data;

/**
 * 입력데이터의 객체 클래스
 * JSON을 객체로 변환시 사용
 */
public class InstObj {
    private float[][][][] instances;

    public float[][][][] getInstances() {
        return instances;
    }

    public void setInstances(float[][][][] instances) {
        this.instances = instances;
    }
}
