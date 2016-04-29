package com.marcestarlet.messaging.broker;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.marcestarlet.messaging.util.BrokerConfiguration;

/**
 * @author MarceStarlet
 * Executes a basic Embedded Broker
 * ActiveMQ
 */
public class EmbeddedBroker {
	
	Logger logger = LoggerFactory.getLogger(EmbeddedBroker.class);
	
	// properties for broker
	private static final String BROKER_NAME = "broker.name";
	// values for broker
	private static final String BROKER_NAME_VALUE = "ActiveMQBroker";
	
	// properties for connectors
	private static final String CONNECTOR_HOST = "connector.host";
	private static final String CONNECTOR_PORT = "connector.port";
	// values for connectors
	private static final String CONNECTOR_HOST_VALUE = "localhost";
	private static final String CONNECTOR_PORT_VALUE = "61616";
	
	// properties for persistence
	private static final String PERSISTENCE_ENABLE = "persistence.enable";
	private static final String PERSISTENCE_KAHADB = "persistence.adapter";
	// values for persistence
	private static final boolean PERSISTENCE_ENABLE_VALUE = true;
	private static final String PERSISTENCE_KAHADB_VALUE = "KAHADB";
	private static final String PERSISTENCE_KAHADB_DIRECTORY = "." + FileSystems.getDefault().getSeparator() + "data";
	private static final String PERSISTENCE_MEMORY_VALUE = "MEMORY";
	
	private final BrokerService brokerService = new BrokerService();
	private BrokerConfiguration cfg;
	private String[] connectorNames = {"tcp", "mqtt"};
	
	public EmbeddedBroker(){
		cfg = BrokerConfiguration.getInstance();
	}
	
	/**
	 * Executes the broker
	 */
	public void executeBroker(){
		configureBroker();
		startBroker();
	}
	
	/**
	 * Fully configure a broker service instance 
	 */
	private void configureBroker(){
		// set the broker name
		getBrokerService().setBrokerName(cfg.getProperty(BROKER_NAME, BROKER_NAME_VALUE));
		// enable JMX for monitoring 
		getBrokerService().setUseJmx(true);
		// disable shutdown hook for manual stop() call
		getBrokerService().setUseShutdownHook(false);
		
		// configure transport connectors (lazy mode)
		try {
			getBrokerService().setTransportConnectors(configureTransportConnectors());
		} catch (Exception e) {
			logger.error("Error while configuring the TransportConnectors",e);
		}
		
		// configure persistence 
		try {
			// TODO change adapters
			getBrokerService().setPersistent(
					Boolean.valueOf(cfg.getProperty(PERSISTENCE_ENABLE, 
							String.valueOf(PERSISTENCE_ENABLE_VALUE))));
			getBrokerService().setPersistenceAdapter(
					configurePersistence(cfg.getProperty(PERSISTENCE_KAHADB, PERSISTENCE_KAHADB_VALUE)));
		} catch (IOException e) {
			logger.error("Error while configuring the Persistence",e);
		}
		
	}
	
	/**
	 * start the broker service
	 */
	private void startBroker(){
		logger.info("Starting the broker . . .");
		try {
			while(true){
				getBrokerService().start();
			}
		} catch (Exception e) {
			// if something goes wrong log exception 
			logger.error("Error while starting the BrokerService",e);
		}finally{
			stopBroker();
		}
	}
	
	/**
	 * stop the broker service
	 */
	private void stopBroker(){
		logger.info("Stoping the broker . . .");
		if(null != getBrokerService() && !getBrokerService().isStopping()){
			try {
				getBrokerService().stop();
			} catch (Exception e) {
				logger.error("Error while stoping the BrokerService",e);

			}
		}	
	}
	
	/**
	 * Configure <code>TransporConnectors</code> lazily 
	 * @return List<TransportConnector>
	 * @throws URISyntaxException
	 */
	private List<TransportConnector> configureTransportConnectors() throws URISyntaxException{
		
		List<TransportConnector> transportConnectors = new ArrayList<>();
		
		// iterate the connectorNames 
		for(String name : connectorNames){
			TransportConnector tc = new TransportConnector();
			// construct URI string
			String uri = name + "://" + 
					cfg.getProperty(name+"."+CONNECTOR_HOST, CONNECTOR_HOST_VALUE) 
					+ ":" + cfg.getProperty(name+"."+CONNECTOR_PORT, CONNECTOR_PORT_VALUE);
			tc.setName(name);
			tc.setUri(new URI(uri));
			// add the transport
			transportConnectors.add(tc);
		}
		
		return transportConnectors;
	}
	
	/**
	 * Configure the <code>PersistenceAdapter</code> in a basic factory
	 * @param persistentType
	 * @return PersistenceAdapter
	 */
	private PersistenceAdapter configurePersistence(String persistenceType){
		
		PersistenceAdapter persistenceAdapter = null;
		
		// choose the persistence type
		if(PERSISTENCE_KAHADB_VALUE.equals(persistenceType)){
			persistenceAdapter = new KahaDBPersistenceAdapter();
			persistenceAdapter.setDirectory(new File(PERSISTENCE_KAHADB_DIRECTORY));
		}else if(PERSISTENCE_MEMORY_VALUE.equals(persistenceType)){
			persistenceAdapter = new MemoryPersistenceAdapter();
		}
		
		return persistenceAdapter;
	}

	/**
	 * @return the brokerService
	 */
	private BrokerService getBrokerService() {
		return brokerService;
	}

	public static void main(String[] args) {
		EmbeddedBroker activemq = new EmbeddedBroker();
		activemq.executeBroker();
	}

}
