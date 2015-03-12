package com.jesgoo.wharf.core.server;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.jesgoo.wharf.thrift.wharfconn.WharfConnService;
import com.jesgoo.wharf.thrift.wharfconn.WharfConnService.Iface;

public class WharfConnServer {
	private int server_port;
	private TServer server = null;
    private int maxWorker = 20;
	public static void main(String[] args) {
		// TProcessor tprocessor = new WharfConnService.Processor<Iface>(
		// new WharfConnImpl());
		// new Thread(new WharfConnServer(8990,tprocessor)).start();
		// System.out.println("WharfConnServer had run");
	}

	TProcessor tprocessor = null;

	public WharfConnServer(int port,int maxWorker) {
		this.server_port = port;
		this.maxWorker = maxWorker;
	}
	
	public WharfConnServer(int port) {
		this.server_port = port;
	}

	public void init(Iface ifc) {
		if (tprocessor == null) {
			tprocessor = new WharfConnService.Processor<Iface>(ifc);
		}
		init();
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
		} catch (TTransportException e) {
			e.printStackTrace();
		}
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
