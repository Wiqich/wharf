package com.jesgoo.wharf.core.client;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.jesgoo.wharf.thrift.wharfconn.Request;
import com.jesgoo.wharf.thrift.wharfconn.Response;
import com.jesgoo.wharf.thrift.wharfconn.WharfConnService;

public class WharfConnClient {
	private int server_port;
	private String host = "127.0.0.1";
	private int timeout = 60000;
	private WharfConnService.Client client = null;

	public static void main(String[] args) throws Exception {
		WharfConnClient client = new WharfConnClient(8990);
		System.out.println(client.ping());
	}

	public WharfConnClient(int port) throws Exception {
		this.server_port = port;
		init();
	}

	public WharfConnClient(String host, int port) throws Exception {
		this.server_port = port;
		this.host = host;
		init();
	}

	public WharfConnClient(String host, int port, int timeout) throws Exception {
		this.server_port = port;
		this.timeout = timeout;
		this.host = host;
		init();
	}
	TTransport transport = null;
	private void init() throws TException {
		transport = new TSocket(host,server_port, timeout);
		TProtocol protocol = new TCompactProtocol(transport);
		client = new WharfConnService.Client(protocol);
		transport.open();
	}
	public void close(){
		if(transport != null){
			transport.close();
			transport = null;
		}
		client = null;
	}

	public boolean ping() throws Exception {
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

	public Response hello(Request req) throws TException {
		if (client == null) {
			this.init();
		}
		return client.hello(req);
	}
}
