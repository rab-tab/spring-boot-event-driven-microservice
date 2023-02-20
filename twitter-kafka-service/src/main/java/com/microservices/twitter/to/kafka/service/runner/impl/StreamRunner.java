package com.microservices.twitter.to.kafka.service.runner.impl;

import twitter4j.Status;
import twitter4j.TwitterException;

public interface StreamRunner {
    void start() throws TwitterException;

    void onStatus(Status status);
}
