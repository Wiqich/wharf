package com.jesgoo.wharf.core.server;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;

import com.jesgoo.wharf.thrift.wharfdata.WharfDataService;

public class WharfDataServer {
	private int server_port;
	private TServer server;

	public static void main(String[] args) {
		// TProcessor tprocessor = new WharfDataService.Processor<Iface>(
		// new WharfDataImpl());
		// new Thread(new WharfDataServer(8991,tprocessor)).start();
		// System.out.println("WharfDataServer had run");
	}

	TProcessor tprocessor = null;

	public WharfDataServer(int port) {
		this.server_port = port;
	}

	public void init() {
		TNonblockingServerSocket tnbSocketTransport;
		try {
			tnbSocketTransport = new TNonblockingServerSocket(server_port);
			TNonblockingServer.Args tnbArgs = new TNonblockingServer.Args(
					tnbSocketTransport);

			tnbArgs.processor(tprocessor);
			tnbArgs.transportFactory(new TFramedTransport.Factory());
			tnbArgs.protocolFactory(new TCompactProtocol.Factory());

			server = new TNonblockingServer(tnbArgs);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void init(WharfDataService.Iface ifc) {
		if (tprocessor == null) {
			tprocessor = new WharfDataService.Processor<WharfDataService.Iface>(
					ifc);
		}
		init();
	}

	public void start() {
		try {
			if (server != null) {
				server.serve();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() {
		if (server != null) {
			server.stop();
		}
	}
}
