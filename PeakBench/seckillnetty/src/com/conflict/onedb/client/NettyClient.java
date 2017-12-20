package com.conflict.onedb.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyClient implements Runnable{
	private ArrayList<String> hosts = new ArrayList<String>();
	private ArrayList<String> ports = new ArrayList<String>();
	private Bootstrap b = null;
	private static ArrayList<Channel> channels = new ArrayList<Channel>();
	private static volatile boolean allWritable = false;
	public NettyClient(ArrayList<String> hosts, ArrayList<String> ports) {
		super();
		this.hosts = hosts;
		this.ports = ports;
		try {
			connect();
		} catch (Exception e) {
//			e.printStackTrace();
		}
	}

    public void connect() throws Exception 
    {
    	System.out.println("RLnettyclient");
		// 配置客户端NIO线程组
		EventLoopGroup group = new NioEventLoopGroup();
	    b = new Bootstrap();
	    b.group(group).channel(NioSocketChannel.class)
		    .option(ChannelOption.TCP_NODELAY, true)
		    .handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch)
				throws Exception {
			    //处理半包
				ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
				//protobuf编解码
				ch.pipeline().addLast(new ProtobufDecoder(MessageProto.
						Message.getDefaultInstance()));
				ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
				ch.pipeline().addLast(new ProtobufEncoder());
				//IO事件的处理类
			    ch.pipeline().addLast(new NettyClientHandler());
			}
		    });
	    //发起异步链接操作
	    for (int i = 0; i < ports.size(); i++) { 
	    	System.out.println("!!!!!!!"+hosts.get(i)+"!!!!!!!!!!");
	    	Channel serverChannel = null;
	    	try{
	    		serverChannel = b.connect(hosts.get(i), Integer.parseInt(ports.get(i))).sync().channel();
	    		allWritable = true;
	    		System.out.println("###Channel#######open"+serverChannel); 
	    	}catch(Exception e){	
	    		System.out.println("@@@@@@2Channel#######not open");
	    	}
	    	channels.add(serverChannel);  
	    } 
	    System.out.println("###########"+channels.size());
    }
  	public static void WorkOrder(MessageProto.Message workOrder, int host) {
//		System.out.println("!!!!!!!"+host+"!!!!!!!!!!"+workOrder.getMsgID()+"!!!!"+workOrder.getType());
		if(channels.get(host)!=null&&NettyClient.isAllWritable())
		{
			channels.get(host).writeAndFlush(workOrder);   
		}
//		else
//		{
//			try {
//				while(true)
//				{
//					Thread.sleep(500);
//					if(channels.get(host)!=null&&NettyClient.isAllWritable())
//					{
//						channels.get(host).writeAndFlush(workOrder); 
//						break;
//					}
//				}
//			} catch (InterruptedException e) {
//			}
//		}
  	}
  	
    public void run() {
    	Channel serverChannel = null;
		while(true) {
			try {
				Thread.sleep(1000);
				for(int i = 0; i < channels.size(); i++) {
					if(channels.get(i)==null) {
						allWritable = false;
						serverChannel = b.connect(hosts.get(i), Integer.
								parseInt(ports.get(i))).sync().channel();
						channels.set(i, serverChannel);
						allWritable = true;
						System.out.println("###Channel#######reopen11111");
					}
					else
					{
						if(!channels.get(i).isWritable())
						{
							allWritable = false;
							channels.set(i, b.connect(hosts.get(i), Integer.
									parseInt(ports.get(i))).sync().channel());
							allWritable = true;
							System.out.println("###Channel#######reopen22222");
						}
					}
				}
			} catch (Exception e) {
			}
		}
	}

	public static boolean isAllWritable() {
		return allWritable;
	}
}
