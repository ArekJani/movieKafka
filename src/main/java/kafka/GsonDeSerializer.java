package kafka;

import com.google.gson.Gson;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class GsonDeSerializer<T> implements Deserializer<T> {

    Gson gson = new Gson();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        String json = new String(bytes, StandardCharsets.UTF_8);
        return (T) new Gson().fromJson(json, KafkaItem.class);
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        String json = new String(data, StandardCharsets.UTF_8);
        KafkaItem item = new Gson().fromJson(json, KafkaItem.class);

        return (T) item;
    }

    @Override
    public T deserialize(String topic, Headers headers, ByteBuffer data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
