package com.kshitij.pocs.kafka.twitterClient;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterLibrary {
    private Hosts hosebirdHosts;
    private StatusesFilterEndpoint hosebirdEndpoint;
    //private List<Long> followings;
    //private List<String> terms;
    private BlockingQueue<String> msgQueue;
    private BlockingQueue<Event> eventQueue;
    private Authentication hosebirdAuth;
    public TwitterLibrary(ArrayList<Long> followings, ArrayList<String> terms,
                            String consumerKey,
                            String consumerSecret,
                            String token,
                            String tokenSecret,
                            LinkedBlockingQueue<String> msgQueue,
                            LinkedBlockingQueue<Event> eventQueue){
        this.hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        this.hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        //this.followings = followings;//Lists.newArrayList(1234L, 566788L);
        //this.terms = terms;//Lists.newArrayList("twitter", "api");
        //this.hosebirdEndpoint.followings(followings);
        this.hosebirdEndpoint.trackTerms(terms);
        this.msgQueue = msgQueue;//new LinkedBlockingQueue<String>(100000);
        this.eventQueue = eventQueue;//new LinkedBlockingQueue<Event>(1000);
        this.hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);
    }

    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */

    public Client getTwitterClient(){
        return clientBuildergetClient();
    }


    // These secrets should be read from a config file

    private Client clientBuildergetClient(){

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(this.hosebirdHosts)
                .authentication(this.hosebirdAuth)
                .endpoint(this.hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(this.msgQueue));
                //.eventMessageQueue(this.eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
        // Attempts to establish a connection.
        //hosebirdClient.connect();
    }
}
