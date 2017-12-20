package com.phei.netty.skread.server;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;


public class RLReturnNettyClient implements Runnable
{
	private String host = null;
	private String port = null;
	private Bootstrap b = null;
	private static Channel channel = null;
	private static boolean allWritable = false;
	public RLReturnNettyClient(String host, String port) {
		super();
		this.host = host;
		this.port = port;
    	try{
			Thread.sleep(5000);
		}catch (InterruptedException e1){
			e1.printStackTrace();
		}
		try {
			connect();
		} catch (Exception e) {
//			e.printStackTrace();
		}
	}

    public void connect() throws Exception 
    {
    	System.out.println("RLReturnnettyclient");
    	System.out.println("*********"+host+"*****"+port);
		// 配置客户端NIO线程组
		EventLoopGroup group = new NioEventLoopGroup();
	    b = new Bootstrap();
	    b.group(group).channel(NioSocketChannel.class)
		    .option(ChannelOption.TCP_NODELAY, true)
		    .handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch)
				throws Exception {
			    ch.pipeline().addLast(new LineBasedFrameDecoder(1024));
			    ch.pipeline().addLast(new StringDecoder());
			    //处理半包
				ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
				//protobuf编解码
				ch.pipeline().addLast(new ProtobufDecoder(MessageReadReturnProto.
						MessageReadReturn.getDefaultInstance()));
				ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
				ch.pipeline().addLast(new ProtobufEncoder());
				//IO事件的处理类
			    ch.pipeline().addLast(new RLReturnNettyClienthandler());
			}
		    });
	    //发起异步链接操作
	    try{
	    	channel = b.connect(host, Integer.parseInt(port)).sync().channel();
	    	allWritable = true;
	    	System.out.println("###ReturnChannel#######open");
	    }catch(Exception e){
	    	
	    }    
	    
    }
    
  	public static void routeWorkOrder(MessageReadReturnProto.MessageReadReturn workOrder) {
  		if(channel!=null&&RLReturnNettyClient.isAllWritable())
  			channel.writeAndFlush(workOrder);
  	}
  	
    public void run() {
		while(true) {
			try {
				Thread.sleep(100);
				if(channel==null)
				{
					allWritable = false;
					channel = b.connect(host, Integer.parseInt(port)).sync().channel();
					allWritable = true;
				}
				else
				{
					if(!channel.isWritable()) {
						allWritable = false;
						channel = b.connect(host, Integer.parseInt(port)).sync().channel();
						allWritable = true;
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
