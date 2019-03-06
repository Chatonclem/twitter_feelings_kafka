package eu.nyuu.courses.model;

public class SensorEvent {
    private String id;
    private String timestamp;
    private String nick;
    private String body;

    public String getNick() {
        return nick;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getId() {
        return id;
    }

    public String getBody() {
        return body;
    }
}
