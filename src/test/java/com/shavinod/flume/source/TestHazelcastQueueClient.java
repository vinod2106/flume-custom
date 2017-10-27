package com.shavinod.flume.source;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;

public class TestHazelcastQueueClient {
	public static final Logger logger = LoggerFactory.getLogger(TestHazelcastQueueClient.class);
	private HazelcastInstance hazelcastClient;
	private BlockingQueue<String> distributedQueue;
	private InetAddress ip;

	private void init(String queueName) throws UnknownHostException {
		// TODO Auto-generated method stub
		logger.info("queueName =======> " + queueName);
		ip = InetAddress.getLocalHost();
		logger.info("ip =======> " + ip);
		String hostname = ip.getHostName();
        logger.info("hostname =======> " + hostname);
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
		clientConfig.getNetworkConfig().addAddress(hostname);
		hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig);
		distributedQueue = hazelcastClient.getQueue(queueName);
	}

	@Test
	public void test() throws NumberFormatException, InterruptedException, UnknownHostException {
		// Send the messages to queue
		TestHazelcastQueueClient hazelcastQueueClient = new TestHazelcastQueueClient();
		hazelcastQueueClient.init("queue");
		hazelcastQueueClient.sendTextMessages(Integer.parseInt("5"));
	}

	private void sendTextMessages(int numOfTimes) throws InterruptedException {
		// TODO Auto-generated method stub

		for (int i = 0; i < numOfTimes; i++) {
			distributedQueue.add("msg#" + i);

		}

	}

}
