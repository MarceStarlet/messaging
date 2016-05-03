/**
 * 
 */
package com.marcestarlet.messaging.client.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MQTT Subscriber using Paho API
 * Protocol MQTTv3
 * Use default values or pass new values to the main method
 * @author MarceStarlet
 *
 * Messaging
 */
public class MQTTPahoSubscriber implements MqttCallback{
	
	// using slf4j - NOTE: it is not provided with Paho add to your classpath or in the maven dependencies
	private static Logger logger = LoggerFactory.getLogger(MQTTPahoPublisher.class);

	/*
	 *  attributes for connection
	 */
	// connect default to eclipse broker
	private String uri = "tcp://iot.eclipse.org:1883";
	// default clientId
	// remember that must be unique for each client if you want to keep a session
	private String clientId = "JavaSampleSubscriber";
	
	/*
	 *  attributes for publish/subscribe
	 */
	// use a default test topic name
	private String topic = "test";
	// Fire & Forget default - do not wait for acknowledge
	// to know more about QoS see 
	private int qos = 0;
	
	public MQTTPahoSubscriber(){
	}
	
	public void executeClient(){
		MqttClient sampleSubscriber = null;
		try {
			// set default memory persistence in case QoS is 1 or 2
			MemoryPersistence persistence = new MemoryPersistence();
			// create new MQTT Client using URI, CLientID and persistence
			sampleSubscriber = new MqttClient(getUri(), getClientId(), persistence);
			// create MQTT Options to set the connection attributes
			MqttConnectOptions options = new MqttConnectOptions();
			// default cleanSession is set to true to indicate the broker to don't keep session
			// change to false if you want the broke keep a session
			options.setCleanSession(true);
			// set 30 sec of keepAlive
			options.setKeepAliveInterval(30);
			
			logger.info("Connecting to: {}", getUri());
			// establish a connection 
			// a CONNECT is sent and the broker should respond with a CONNACK
			sampleSubscriber.connect(options);
			// set an asynchronous receive
			sampleSubscriber.setCallback(this);
			
			// subscribe to the Topic and QoS indicated 
			sampleSubscriber.subscribe(getTopic(), getQos());
			logger.info("Subscribed and waiting ...");
			
		} catch (MqttException e) {
			// handle exception
			logger.error("Error while susbcribing");
			logger.error("Reason ", e.getReasonCode());
			logger.error("Message ", e.getMessage());
			logger.error("Localized Message ", e.getLocalizedMessage());
			logger.error("Cause ", e.getCause());
			logger.error("Exception ", e);
			logger.error("StackTrace", (Object[])e.getStackTrace());
		} finally { 
			if(null != sampleSubscriber) {
				try {
					sampleSubscriber.disconnect();
				} catch (MqttException e) {
					logger.error("Error while disconnecting",e);
				}
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see org.eclipse.paho.client.mqttv3.MqttCallback#connectionLost(java.lang.Throwable)
	 */
	@Override
	public void connectionLost(Throwable cause) {
		logger.error("Connection lost");
		logger.error("Cause ", cause);
	}

	/* (non-Javadoc)
	 * @see org.eclipse.paho.client.mqttv3.MqttCallback#messageArrived(java.lang.String, org.eclipse.paho.client.mqttv3.MqttMessage)
	 */
	@Override
	public void messageArrived(String topic, MqttMessage message)
			throws Exception {
		logger.info("Message arrived: {}", new String(message.getPayload()));	
	}

	/* (non-Javadoc)
	 * @see org.eclipse.paho.client.mqttv3.MqttCallback#deliveryComplete(org.eclipse.paho.client.mqttv3.IMqttDeliveryToken)
	 */
	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		logger.info("Delivery Completed!");
	}

   /**
	 * @return the uri
	 */
	public String getUri() {
		return uri;
	}

	/**
	 * @param uri the uri to set
	 */
	public void setUri(String uri) {
		this.uri = uri;
	}

	/**
	 * @return the clientId
	 */
	public String getClientId() {
		return clientId;
	}

	/**
	 * @param clientId the clientId to set
	 */
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	/**
	 * @return the topic
	 */
	public String getTopic() {
		return topic;
	}

	/**
	 * @param topic the topic to set
	 */
	public void setTopic(String topic) {
		this.topic = topic;
	}

	/**
	 * @return the qos
	 */
	public int getQos() {
		return qos;
	}

	/**
	 * @param qos the qos to set
	 */
	public void setQos(int qos) {
		this.qos = qos;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MQTTPahoSubscriber client = new MQTTPahoSubscriber();
		if(args.length <= 0){
			logger.info("No parameters set, running as default");
			client.executeClient();
		}else{
			logger.info("Running with parameters");
			client.setUri(args[0]);
			client.setClientId(args[1]);
			client.setTopic(args[2]);
			client.setQos(Integer.parseInt(args[3]));
			client.executeClient();
		}

	}
}
