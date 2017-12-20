package com.phei.netty.onedb.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import com.phei.netty.onedb.client.MessageProto;

public class NettyServer implements Runnable {
	public String port ;
	NettyServer(String port)
	{
		super();
		this.port =port;
	}
	
    public void bind(int port) throws Exception {
    System.out.println("~~~~~~~RLnettyserver~~~~~~~~~"+port);
	// 配置服务端的NIO线程组
	EventLoopGroup bossGroup = new NioEventLoopGroup();
	EventLoopGroup workerGroup = new NioEventLoopGroup();
	try {
	    ServerBootstrap b = new ServerBootstrap();
	    b.group(bossGroup, workerGroup)
	    .channel(NioServerSocketChannel.class)
	    .option(ChannelOption.SO_BACKLOG, 100)
	    .handler(new LoggingHandler(LogLevel.INFO))
	    .childHandler(new ChannelInitializer<SocketChannel>() {
		@Override
		public void initChannel(SocketChannel ch) {
			//处理半包问题
			ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
			//解码&编码
			ch.pipeline().addLast(new ProtobufDecoder(MessageProto.Message.
					getDefaultInstance()));
			ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
			ch.pipeline().addLast(new ProtobufEncoder());
			ch.pipeline().addLast(new NettyServerHandler());
		}
	    });
	    // 绑定端口，同步等待成功
	    ChannelFuture f = b.bind(port).sync();
	    System.out.println("~~~~~~~RLnettyserver open~~~~~~~~~"+port);
	    // 等待服务端监听端口关闭
	    f.channel().closeFuture().sync();
	} finally {
	    // 优雅退出，释放线程池资源
	    bossGroup.shutdownGracefully();
	    workerGroup.shutdownGracefully();
	}
    }

	@Override
	public void run()
	{
	    try
		{
			this.bind(Integer.parseInt(port));
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}		
	}
}

