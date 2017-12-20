/*
 * Copyright 2016-12-12 Zhangchunxi.

 */
package test;

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

public class RLNettyClient {
	private String hosts = null;
	private String ports = null;
	private Bootstrap b = null;
	private static Channel channel = null; 
	public RLNettyClient(String hosts, String ports) {
		super();
		this.hosts = hosts;
		this.ports = ports;
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
				ch.pipeline().addLast(new ProtobufDecoder(MessageReadProto.
						MessageRead.getDefaultInstance()));
				ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
				ch.pipeline().addLast(new ProtobufEncoder());
				//IO事件的处理类
			    ch.pipeline().addLast(new RLNettyClientHandler());
			}
		    });
	    //发起异步链接操作
	    	System.out.println("!!!!!!!"+hosts+"!!!!!!!!!!");
	    	 channel = b.connect(hosts, Integer.parseInt(ports)).sync().channel();;
	    	System.out.println("###Channel#######open"); 
	        
	        for(int i = 0; i < 10; i++)
	        {
	        	System.out.println("HAHAHHAA");
	        	MessageReadProto.MessageRead.Builder workMessage = MessageReadProto.
	        			MessageRead.newBuilder();
	        	workMessage.setMsgID(1);
	        	workMessage.setType(1);
	        	workMessage.setItemID(1);
	        	routeWorkOrder(workMessage.build(),0);
	        }
    }
    
  	public static void routeWorkOrder(MessageReadProto.MessageRead workOrder, int host) {
  			System.out.println("!!!!!!!"+host+"!!!!!!!!!!"+workOrder.getMsgID()+"!!!!"+workOrder.getType());
  			channel.writeAndFlush(workOrder);   
  	}
	public static void main(String[] args) {
		RLNettyClient a =new RLNettyClient("127.0.0.1", "21880");
		try
		{
			a.connect();
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
  	}
}
