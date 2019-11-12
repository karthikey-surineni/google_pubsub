import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import com.google.api.core.ApiFutures;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

public class PubSubTest {

    private String topicId = "pubsub-test-topic";
    private String projectId = "cb-dataflow-python";
    private Publisher publisher;


    public PubSubTest() {

        GoogleCredentials credentials;
        File credentialsPath = new File("res/cb-dataflow-python-23cc97500072.json");
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {

            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);

            ProjectTopicName topicName = ProjectTopicName.of(this.projectId, this.topicId);

            // Create a publisher instance with default settings bound to the topic
             this.publisher = Publisher.newBuilder(topicName)
                                            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                                            .build();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void publishMessage(List<String> messages){
        List<ApiFuture<String>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < messages.size(); i++) {
                String message = messages.get(i);

                // convert message to bytes
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                        .setData(data)
                        .build();

                // Schedule a message to be published. Messages are automatically batched.
                ApiFuture<String> future = this.publisher.publish(pubsubMessage);
                futures.add(future);
            }
        } finally {
            /* Wait on any pending requests */
            List<String> messageIds = null;
            try {
                messageIds = ApiFutures.allAsList(futures).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

            for (String messageId : messageIds) {
                System.out.println(messageId);
            }

            if (this.publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                try {
                    this.publisher.shutdown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        PubSubTest pTest = new PubSubTest();

        List<String> strings = Arrays.asList("foo", "bar", "baz");
        pTest.publishMessage(strings);
    }

}
