package com.marcestarlet.messaging.client.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MQTT Publisher using Paho API
 * Protocol MQTTv3
 * Use default values or pass new values to the main method
 * @author MarceStarlet
 *
 * Messaging
 */
public class MQTTPahoPublisher {
	
	// using slf4j - NOTE: it is not provided with Paho add to your classpath or in the maven dependencies
	private static Logger logger = LoggerFactory.getLogger(MQTTPahoPublisher.class);

	/*
	 *  attributes for connection
	 */
	// connect default to eclipse broker
	private String uri = "tcp://iot.eclipse.org:1883";
	// default clientId
	// remember that must be unique for each client if you want to keep a session
	private String clientId = "JavaSamplePublisher";
	
	/*
	 *  attributes for publish
	 */
	// use a default test topic name
	private String topic = "test";
	// Fire & Forget default - do not wait for acknowledge
	// to know more about QoS see 
	private int qos = 0;
	// the content of the message
	private String payload = "Sample Message, Hello Wordl!";
	
	public MQTTPahoPublisher(){
	}
	
	public void executeClient(){
		MqttClient samplePublisher = null;
		try {
			// set default memory persistence in case QoS is 1 or 2
			MemoryPersistence persistence = new MemoryPersistence();
			// create new MQTT Client using URI, CLientID and persistence
			samplePublisher = new MqttClient(getUri(), getClientId(), persistence);
			// create MQTT Options to set the connection attributes
			MqttConnectOptions options = new MqttConnectOptions();
			// default cleanSession is set to true to indicate the broker to don't keep session
			// change to false if you want the broke keep a session
			options.setCleanSession(true);
			logger.info("Connecting to: {}", getUri());
			// establish a connection 
			// a CONNECT is sent and the broker should respond with a CONNACK
			samplePublisher.connect(options);
			
			// create new MQTT message
			MqttMessage message = new MqttMessage();
			// the payload is the content of a message 
			message.setPayload(getPayload().getBytes());
			logger.info("Publishing message {} to {} topic", getPayload(), getTopic());
			// publish the message to the Topic indicated
			samplePublisher.publish(getTopic(), message);
			logger.info("Message sent, disconnecting client");
			// disconnect
			samplePublisher.disconnect();
			
		} catch (MqttException e) {
			// handle exception
			logger.error("Error while publishing");
			logger.error("Reason ", e.getReasonCode());
			logger.error("Message ", e.getMessage());
			logger.error("Localized Message ", e.getLocalizedMessage());
			logger.error("Cause ", e.getCause());
			logger.error("Exception ", e);
			logger.error("StackTrace", (Object[])e.getStackTrace());
		} 
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
	 * @return the payload
	 */
	public String getPayload() {
		return payload;
	}

	/**
	 * @param payload the payload to set
	 */
	public void setPayload(String payload) {
		this.payload = payload;
	}

	public static void main(String[] args) {
		MQTTPahoPublisher client = new MQTTPahoPublisher();
		if(args.length <= 0){
			logger.info("No parameters set, running as default");
			client.executeClient();
		}else{
			logger.info("Running with parameters");
			client.setUri(args[0]);
			client.setClientId(args[1]);
			client.setTopic(args[2]);
			client.setQos(Integer.parseInt(args[3]));
			client.setPayload(args[4]);
			client.executeClient();
		}
	}

}
