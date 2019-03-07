package eu.nyuu.courses.model;

public class Sentiment {

    private Integer positive;
    private Integer neutral;
    private Integer negative;

    public Sentiment(Integer positive, Integer neutral, Integer negative) {
        this.positive = positive;
        this.neutral = neutral;
        this.negative = negative;
    }
}
