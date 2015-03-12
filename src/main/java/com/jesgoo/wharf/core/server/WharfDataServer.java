package com.jesgoo.wharf.core.server;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

import com.jesgoo.wharf.thrift.wharfdata.WharfDataService;

public class WharfDataServer {
	private int server_port;
	private TServer server;
    private int maxWorker = 5;
	public static void main(String[] args) {
		// TProcessor tprocessor = new WharfDataService.Processor<Iface>(
		// new WharfDataImpl());
		// new Thread(new WharfDataServer(8991,tprocessor)).start();
		// System.out.println("WharfDataServer had run");
	}

	TProcessor tprocessor = null;

	public WharfDataServer(int port,int maxWorker) {
		this.server_port = port;
		this.maxWorker = maxWorker;
	}
	public WharfDataServer(int port) {
		this.server_port = port;
	}

	public void init() {
		TServerSocket serverTransport;
		try {

			serverTransport = new TServerSocket(server_port);
			TThreadPoolServer.Args ttpsArgs = new TThreadPoolServer.Args(
					serverTransport);
			ttpsArgs.maxWorkerThreads(maxWorker);
			ttpsArgs.processor(tprocessor);
			ttpsArgs.protocolFactory(new TCompactProtocol.Factory());
			server = new TThreadPoolServer(ttpsArgs);
			server.serve();

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
