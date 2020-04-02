package com.kshitij.pocs.kafka.twitterClient;

import com.google.common.collect.Lists;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.event.Event;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ReadTwitter {
    Logger logger= LoggerFactory.getLogger(ReadTwitter.class.getName());
    String consumerKey= "";
    String consumerSecret="";
    String token="";
    String tokenSecret="";
    private String bootStrapServer= "localhost:9092";
    private String topic="corona_tweets_1";
    public static void main(String[] args) {

        new ReadTwitter().run();

    }

    private ReadTwitter(){

    }
    private void run(){
        //Create Twitter Client
        //this.followings = followings;//Lists.newArrayList(1234L, 566788L);
        //this.terms = terms;//Lists.newArrayList("twitter", "api");

        LinkedBlockingQueue msgQueue = new LinkedBlockingQueue<String>(100000);
        LinkedBlockingQueue eventQueue = new LinkedBlockingQueue<Event>(1000);
        TwitterLibrary twitterLibrary = new TwitterLibrary(
                                        Lists.newArrayList(1234L, 566788L),
                                        Lists.newArrayList("corona"),
                                        consumerKey,consumerSecret,token,tokenSecret,
                                        msgQueue,eventQueue);
        Client twitterClient= twitterLibrary.getTwitterClient();
        //twitterClient.connect();

        //Create Kafka producer
        KafkaProducer<String,String > producer= createKafkaProducer();



        //send tweets to kafka
        //while(!twitterClient.isDone()){
        while(!Boolean.FALSE){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //String message=null;
            String message="{\n \"Hello\": \"Kshitij"+ LocalTime.now().getNano()+"\"\n}";
            /*try {
                message= (String) msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }*/
            if (message != null){
                logger.info(message);
                ProducerRecord<String,String> producerRecord=new ProducerRecord<String, String>(topic,"covid",message);
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null){
                            logger.error("Something happened",exception);
                        }
                    }
                });
            }
        }
        logger.info("Application Ended");
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Shutting Down twitter");
            twitterClient.stop();
            logger.info("Closing the Producer");
            producer.close();
        }));

        /*RunThread runnable= new RunThread(twitterClient,msgQueue);
        Thread runThread = new Thread(runnable);
        runThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            ((RunThread) runnable).stopThread();
        }));*/
    }
    public KafkaProducer<String,String > createKafkaProducer(){
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Safe configs
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        producerProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        //Batching in Kafka for high throughput and litter more cpu cycles
        producerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"10");
        producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<String, String>(producerProperties);
        return kafkaProducer;

    }


    class RunThread implements Runnable{
        private Client client;
        private LinkedBlockingQueue msgQueue;
        public RunThread(Client client,LinkedBlockingQueue msgQueue){
            this.client=client;
            this.msgQueue=msgQueue;
        }

        @Override
        public void run() {
            while(!this.client.isDone()){
                try {
                    String message=this.msgQueue.take().toString();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    this.client.stop();
                }
            }
        }

        public void stopThread(){
            this.client.stop();
        }
    }
}
