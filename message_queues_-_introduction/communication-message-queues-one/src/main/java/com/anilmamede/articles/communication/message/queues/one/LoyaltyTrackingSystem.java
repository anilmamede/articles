package com.anilmamede.articles.communication.message.queues.one;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.NamingException;

public class LoyaltyTrackingSystem {

	public LoyaltyTrackingSystem() {

	}

	public void run() throws NamingException, JMSException, IOException {
		javax.naming.Context ctx = new javax.naming.InitialContext();

		javax.jms.ConnectionFactory connectionFactory = (javax.jms.ConnectionFactory) ctx.lookup("ConnectionFactory");

		Connection connection = connectionFactory.createConnection();

		final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

		javax.jms.Destination purchasesQueue = (javax.jms.Destination) ctx.lookup("dynamicQueues/purchasesQueue");

		MessageConsumer consumer = session.createConsumer(purchasesQueue);

		final Random random = new Random();
		consumer.setMessageListener(new MessageListener() {

			public void onMessage(Message m) {
				TextMessage tm = (TextMessage) m;

				try {
					System.out.println("Loyalty Tracking System: " + tm.getText());

					int timeToProcess = random.nextInt(2000) + 1000;

					Thread.sleep(timeToProcess);
					session.commit();
				} catch (JMSException e) {
					throw new RuntimeException(e);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

			}
		});

		System.out.println("Loyalty Tracking System is ready to receive messages.");
		System.out.println("Type 'quit' to quit...");
		connection.start();

		while (true) {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

			if (br.ready()) {
				String command = br.readLine();

				if ("quit".equals(command.trim())) {
					break;
				}
			}
		}

		session.close();
		connection.close();
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws JMSException
	 * @throws NamingException
	 */
	public static void main(String[] args) throws NamingException, JMSException, IOException {
		new LoyaltyTrackingSystem().run();
	}

}
