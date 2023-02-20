package com.microservices.twitter.to.kafka.service.listener;


;
import com.microservices.TwitterToKafkaServiceConfigData;
import com.microservices.twitter.to.kafka.service.runner.impl.StreamRunner;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.*;

import java.util.Arrays;

@Component
public class TwitterKafkaStatusListener implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStatusListener.class);

    private final TwitterToKafkaServiceConfigData kafkaConfigData;

    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private TwitterStream twitterStream;

    public TwitterKafkaStatusListener(TwitterToKafkaServiceConfigData kafkaConfigData, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.kafkaConfigData = kafkaConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener((StreamListener) twitterKafkaStatusListener);
        addFilter();
    }
    @Override
    public void onStatus(Status status) {
        LOG.info("Received status text {} sending to kafka topic {}", status.getText());
       //// TwitterAvroModel twitterAvroModel = twitterStatusToAvroTransformer.getTwitterAvroModelFromStatus(status);
       // kafkaProducer.send(kafkaConfigData.getTopicName(), twitterAvroModel.getUserId(), twitterAvroModel);
    }

    private void addFilter() {
        String[] keywords = kafkaConfigData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        LOG.info("Starting mock filtering twitter streams for keywords {}", Arrays.toString(keywords));
    }

    @PreDestroy
    public void shutDown() {
        if (twitterStream != null) {
            LOG.info("Closing twiiter stream");
            twitterStream.shutdown();
        }
    }

}

