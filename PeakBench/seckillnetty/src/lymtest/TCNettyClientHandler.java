package lymtest;

import org.apache.log4j.Logger;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * @author LiYuming
 * @time 20160906
 * @desc ���Զ˿ͻ��˵Ĵ����࣬�����5��Ľ����Ϣ
 */
public class TCNettyClientHandler extends ChannelInboundHandlerAdapter {

	private Logger flowlogger = null;
	private Logger syslogger = null;

	public TCNettyClientHandler() {
		flowlogger = Logger.getLogger("flowlogger");
		syslogger = Logger.getLogger("syslogger");
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) {
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg)
			throws Exception {
		System.out.println("aaaaaaaaaaaaaaa");
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		//ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}
}
