package kafka;

public class KafkaItem {
    private String name;
    private int rating;

    public KafkaItem(String name, int rating) {
        this.name = name;
        this.rating = rating;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

    @Override
    public String toString() {
        return "KafkaItem{" +
                "name='" + name + '\'' +
                ", rating=" + rating +
                '}';
    }
}
