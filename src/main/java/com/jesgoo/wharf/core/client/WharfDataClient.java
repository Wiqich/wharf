package com.jesgoo.wharf.core.client;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.jesgoo.wharf.thrift.wharfdata.Event;
import com.jesgoo.wharf.thrift.wharfdata.WharfDataService;

public class WharfDataClient {
	private int server_port;
	private String host = "127.0.0.1";
	private int timeout = 5000;
	private WharfDataService.Client client = null;

	public static void main(String[] args) throws TTransportException {
		WharfDataClient client = new WharfDataClient(8991);
		System.out.println(client.ping());
	}

	public WharfDataClient(int port) throws TTransportException {
		this.server_port = port;
		init();
	}

	public WharfDataClient(String host, int port) throws TTransportException {
		this.server_port = port;
		this.host = host;
		init();
	}

	public WharfDataClient(String host, int port, int timeout)
			throws TTransportException {
		this.server_port = port;
		this.timeout = timeout;
		this.host = host;
		init();
	}
	TTransport transport = null;
	private void init() throws TTransportException {
		transport = new TSocket(host,server_port, timeout);
		TProtocol protocol = new TCompactProtocol(transport);
		client = new WharfDataService.Client(protocol);
		transport.open();
	}

	public boolean ping() throws TTransportException {
		if (client == null) {
			this.init();
		}
		try {
			return client.ping();
		} catch (TException e) {
			e.printStackTrace();
			close();
			return false;
		}
	}

	public void close(){
		if(transport != null){
			transport.close();
			transport = null;
		}
		client = null;
	}
	
	public boolean push(Event ev) throws TException {
		if (client == null) {
			this.init();
		}
	    return client.push(ev);
	}
}
