package org.nebula.javar;

import java.util.List;

import org.nebula.cfs.CFSWriter;

import redis.clients.jedis.Jedis;

public class AnalyticsConsumer extends Thread {
	private final Jedis consumer;
	private final String topic;
	private final CFSWriter writer;

	public AnalyticsConsumer() {
		this.consumer = new Jedis("localhost");
		this.topic = Topics.RAW_EVENTS;
		this.writer = new CFSWriter("2014-02-18-11");
	}

	public void run() {
		
		System.out.println(String.format("Consuming '%s'", this.topic));
		System.out.println(this.consumer.keys("events"));
		while (true) {
			List<String> top = this.consumer.blpop(0, this.topic);
			String message = top.get(1);
//			System.out.println(message);
			writer.append(message);
		}

	}
}
