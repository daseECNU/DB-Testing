package test;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class RLNettyClientHandler extends ChannelInboundHandlerAdapter
{
	public RLNettyClientHandler()
	{
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx){
//		System.out.println("*****write********");
//		MessageReadProto.MessageRead.Builder workMessage = MessageReadProto.
//				MessageRead.newBuilder();
//        workMessage.setMsgID(1);
//        workMessage.setType(1);
//        workMessage.setItemID(1);
//        ctx.write(workMessage);
//        ctx.flush();
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
	{
		MessageReadProto.MessageRead message = (MessageReadProto.MessageRead) msg;
		if(message.getType() == 1)
		{
			System.out.println(message.getType());
		}
		else if(message.getType() == 2)
		{
			System.out.println(message.getType());
		}
		else if(message.getType() == 3)
		{
			System.out.println(message.getType());
		}
		else if(message.getType() == 4)
		{
			System.out.println(message.getType());
		}
		else if(message.getType() == 5)
		{
			System.out.println(message.getType());
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
	{
		// 释放资源
		ctx.close();
	}
}
