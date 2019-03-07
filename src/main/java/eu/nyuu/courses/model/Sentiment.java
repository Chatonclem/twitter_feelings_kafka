package eu.nyuu.courses.model;

public class Sentiment {

    private Integer positive;
    private Integer neutral;
    private Integer negative;

    public Integer getPositive() {
        return positive;
    }

    public void setPositive(Integer positive) {
        this.positive = positive;
    }

    public Integer getNeutral() {
        return neutral;
    }

    public void setNeutral(Integer neutral) {
        this.neutral = neutral;
    }

    public Integer getNegative() {
        return negative;
    }

    public void setNegative(Integer negative) {
        this.negative = negative;
    }

    public Sentiment(Integer positive, Integer neutral, Integer negative) {
        this.positive = positive;
        this.neutral = neutral;
        this.negative = negative;
    }

    public Sentiment() {}

    @Override
    public String toString() {
        return String.format("Positifs : %s , Neutres : %s, Negatifs : %s", this.positive, this.neutral, this.negative);
    }
}
