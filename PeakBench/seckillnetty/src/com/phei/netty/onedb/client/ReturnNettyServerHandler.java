package com.phei.netty.onedb.client;

import com.phei.netty.onedb.server.MessageReturnProto;

import util.Operation;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ReturnNettyServerHandler extends ChannelInboundHandlerAdapter
{
	public void channelRead(ChannelHandlerContext ctx, Object msg)
	{
		try
		{
			System.out.println("WLnettyreturnserver~~~~~~");
			MessageReturnProto.MessageReturn message = (MessageReturnProto.MessageReturn) msg;
			Operation<MessageReturnProto.MessageReturn> operation = new Operation<MessageReturnProto.MessageReturn>(ctx,  message);
			ResultStatistic.addOperation(operation);
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
