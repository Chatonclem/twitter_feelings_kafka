package eu.nyuu.courses.model;

/**
 * Created by Charles on 06/03/2019.
 */
public class TweetSentiment {

        private String body;
        private String sentiment;

    public TweetSentiment(String body, String sentiment) {
        this.body = body;
        this.sentiment = sentiment;
    }

    public String getBody() {
        return body;
    }

    public String getSentiment() {
        return sentiment;
    }
}
