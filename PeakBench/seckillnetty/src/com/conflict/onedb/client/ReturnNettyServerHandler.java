package com.conflict.onedb.client;

import util.Operation;

import com.conflict.onedb.server.MessageReturnProto;

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
