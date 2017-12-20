package lymtest;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * @author LiYuming
 * @time 20160906
 * @desc ���Զ˷�����Ĵ�����
 */
public class TCNettyServerHandler extends ChannelInboundHandlerAdapter{


	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg)
			throws Exception {
		System.out.println("AAAAAAAAAAAAAAA");
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		//�����쳣���ر�t��
		ctx.close();
	}
}
