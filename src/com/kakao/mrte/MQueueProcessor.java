package com.kakao.mrte;

import com.rabbitmq.client.Channel;

public class MQueueProcessor implements Runnable {
    long tag;
    Channel chan;
    MQueueConsumer worker;

    MQueueProcessor(MQueueConsumer w, long t, Channel c) {
        worker = w;
        tag = t;
        chan = c;
    }

    public void run() {
    	/*
        try {
            // .... doing something
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        */

        // Do not send ack
        /*
        if (chan.isOpen()) {
            try {
                chan.basicAck(tag, false);
                worker.processed++;
            } catch (IOException e) {}
        }
        */
    }
}