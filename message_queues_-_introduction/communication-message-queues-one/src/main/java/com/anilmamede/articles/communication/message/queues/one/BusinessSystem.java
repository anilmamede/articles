package com.anilmamede.articles.communication.message.queues.one;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.NamingException;

public class BusinessSystem {

	private String name;

	public BusinessSystem(final String name) {
		this.setName(name);
	}

	public void run() throws NamingException, JMSException, IOException, InterruptedException {
		javax.naming.Context ctx = new javax.naming.InitialContext();

		javax.jms.ConnectionFactory connectionFactory = (javax.jms.ConnectionFactory) ctx.lookup("ConnectionFactory");

		Connection connection = connectionFactory.createConnection();

		Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

		javax.jms.Destination purchasesQueue = (javax.jms.Destination) ctx.lookup("dynamicQueues/purchasesQueue");

		MessageProducer producer = session.createProducer(purchasesQueue);

		System.out.println("System " + getName() + " is ready to notify purchases.");
		System.out.println("Type 'quit' to quit...");

		Random random = new Random();
		int clientCounter = 0;

		while (true) {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

			if (br.ready()) {
				String command = br.readLine();

				if ("quit".equals(command.trim())) {
					break;
				}
			}

			int purchaseValue = random.nextInt(100);
			String purchaseMessage = getName() + " registered a purchase of " + purchaseValue
					+ " dollares from Client " + clientCounter++;

			TextMessage textMessage = session.createTextMessage(purchaseMessage);
			producer.send(textMessage);
			session.commit();

			System.out.println(purchaseMessage);

			Thread.sleep(2000);
		}

		session.close();
		connection.close();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @param args
	 * @throws JMSException
	 * @throws NamingException
	 */
	public static void main(String[] args) throws NamingException, JMSException, IOException, InterruptedException {
		String systemName = args[0].trim();

		BusinessSystem businessSystem = new BusinessSystem(systemName);
		businessSystem.run();
	}

}
