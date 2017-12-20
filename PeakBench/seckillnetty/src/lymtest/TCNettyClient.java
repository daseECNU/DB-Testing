package lymtest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Vector;

import org.apache.log4j.Logger;

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

/**
 * @author LiYuming
 * @time 20160906
 * @desc ���Զ˿ͻ��ˣ������5���Ϣ
 */
public class TCNettyClient {

	private ArrayList<String> hosts = null;
	private ArrayList<String> ports = null;
	private Bootstrap b = null;

	public TCNettyClient(ArrayList<String> hosts, ArrayList<String> ports) {
		this.hosts = hosts;
		this.ports = ports;
		try {
			init();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//����PN��t��ͨ��
	private static Vector<Channel> channels = new Vector<Channel>();
	private Logger syslogger = Logger.getLogger("syslogger");

	//�ͻ��˰󶨶��PN
	private void init() throws Exception{
		// ���ÿͻ���NIO�߳���
		EventLoopGroup group = new NioEventLoopGroup();
		//Bootstrap: �ͻ��˸�������
		b = new Bootstrap();
		b.group(group).channel(NioSocketChannel.class)
		.option(ChannelOption.TCP_NODELAY, true)
		.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch)
					throws Exception {
				//������
				ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
				ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
				//protobuf�����
				ch.pipeline().addLast(new ProtobufDecoder(MessageReadProto.MessageRead.
						getDefaultInstance()));
				ch.pipeline().addLast(new ProtobufEncoder());
				//IO�¼��Ĵ�����
				ch.pipeline().addLast(new TCNettyClientHandler());
			}
		});
		// �����첽t�Ӳ���
		for(int i = 0; i < hosts.size(); i++) {
			// �󶨶˿ڣ����ȴ�ͬ���ɹ�
			channels.add(b.connect(hosts.get(i), Integer.parseInt(ports.get(i))).
					sync().channel());
		}
		System.out.println("bbbbbbbbbbbb");
		System.out.println(channels.size());
	}

	public void grabOrder(MessageReadProto.MessageRead grabOrder, int host) {
		channels.get(host).writeAndFlush(grabOrder);
		channels.get(host).flush();
	}

	public static void main(String[] args) {
		ArrayList<String> hosts = new ArrayList<String>(Arrays.asList("127.0.0.1"));
		ArrayList<String> ports = new ArrayList<String>(Arrays.asList("22808"));
		TCNettyClient tc = new TCNettyClient(hosts, ports);
		
		for(int i = 0; i < 10; i++)
        {
        	System.out.println("HAHAHHAA");
        	MessageReadProto.MessageRead.Builder workMessage = MessageReadProto.
        			MessageRead.newBuilder();
        	workMessage.setMsgID(10);
        	workMessage.setType(10);
        	workMessage.setItemID(10);
        	tc.grabOrder(workMessage.build(), 0);
        }
	}
}
