package lymtest;

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

/**
 * @author LiYuming
 * @time 20160906
 * @desc ���Զ˷�������4�����ɵ���Ϣ
 */
public class TCNettyServer {
	ChannelFuture cf = null;
	//����Զ˷�����
	public void bind(String serverPort) {
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try {
			//ServerBootstrap��Netty������NIO����˵ĸ�������
			//Ŀ���ǽ��ͷ���˵Ŀ������Ӷ�
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup)
			.channel(NioServerSocketChannel.class)
			//���t����
			.option(ChannelOption.SO_BACKLOG, 1024)
			//netty�Դ��¼��־��Handler
			.handler(new LoggingHandler(LogLevel.INFO))
			.childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) {
					//����������
					ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
					//����&����
					ch.pipeline().addLast(new ProtobufDecoder(MessageReadProto.MessageRead.
							getDefaultInstance()));
					ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
					ch.pipeline().addLast(new ProtobufEncoder());
					ch.pipeline().addLast(new TCNettyServerHandler());
				}
			});
			//�󶨶˿ڣ��ȴ�ͬ���ɹ�
			cf = b.bind(Integer.parseInt(serverPort)).sync();
			System.out.println("aaaaaaaaaaaaaaaaaa");
			System.out.println(cf.isSuccess());
			
			cf.channel().closeFuture().sync();
			System.out.println("bbbbbbbbbbbbbbbbbb");
		} catch(Exception e) { 
			e.printStackTrace();
		} finally {
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
	}
	
	public static void main(String[] args) 
	{
		TCNettyServer aa = new TCNettyServer();
		aa.bind("22808");
		System.out.println("aaaaaaaaaaaa");
	}
}
