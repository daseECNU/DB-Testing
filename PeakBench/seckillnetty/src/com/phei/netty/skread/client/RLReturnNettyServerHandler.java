package com.phei.netty.skread.client;

import util.Operation;

import com.phei.netty.skread.server.MessageReadReturnProto;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class RLReturnNettyServerHandler extends ChannelInboundHandlerAdapter
{
	public void channelRead(ChannelHandlerContext ctx, Object msg)
	{
		try
		{
			MessageReadReturnProto.MessageReadReturn message = (MessageReadReturnProto.MessageReadReturn) msg;
			Operation<MessageReadReturnProto.MessageReadReturn> operation = new Operation<MessageReadReturnProto.MessageReadReturn>(ctx,  message);
			RLResultStatistic.addOperation(operation);
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
