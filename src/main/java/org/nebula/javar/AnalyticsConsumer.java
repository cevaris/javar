package org.nebula.javar;

import java.util.List;

import redis.clients.jedis.Jedis;

public class AnalyticsConsumer extends Thread {
	private final Jedis consumer;
	private final String topic;

	public AnalyticsConsumer() {
		this.consumer = new Jedis("localhost");
		this.topic = Topics.RAW_EVENTS;
	}

	public void run() {
		
		System.out.println(String.format("Consuming '%s'", this.topic));
		while(true){
			List<String> top = this.consumer.blpop(0, this.topic);
			System.out.println(top);	
		}
		
	}
}
