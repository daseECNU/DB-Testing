package com.phei.netty.skwrite.client;

import util.Operation;
import com.phei.netty.skwrite.server.MessageWriteReturnProto;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class WLReturnNettyServerHandler extends ChannelInboundHandlerAdapter
{
	public void channelRead(ChannelHandlerContext ctx, Object msg)
	{
		try
		{
			System.out.println("WLnettyreturnserver~~~~~~");
			MessageWriteReturnProto.MessageWriteReturn message = (MessageWriteReturnProto.MessageWriteReturn) msg;
			Operation<MessageWriteReturnProto.MessageWriteReturn> operation = new Operation<MessageWriteReturnProto.MessageWriteReturn>(ctx,  message);
			WLResultStatistic.addOperation(operation);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) 
	{
		cause.printStackTrace();
		ctx.close();
	}
}