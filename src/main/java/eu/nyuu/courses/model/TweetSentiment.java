package eu.nyuu.courses.model;

/**
 * Created by Charles on 06/03/2019.
 */
public class TweetSentiment {

        private String user;
        private String timestamp;
        private String body;
        private String sentiment;

        public TweetSentiment(){}

        public TweetSentiment(String user, String timestamp, String body, String sentiment) {
            this.user = user;
            this.timestamp = timestamp;
            this.body = body;
            this.sentiment = sentiment;
        }

        public String getUser() { return user;}

        public String getTimestamp() { return timestamp;}

        public String getBody() {
            return body;
        }

        public String getSentiment() {
            return sentiment;
        }
}
