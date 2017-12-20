package com.conflict.onedb.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ReturnNettyClienthandler extends ChannelInboundHandlerAdapter
{
	public ReturnNettyClienthandler()	{
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx){
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
	{
//		MessageReturnProto.MessageReturn message = (MessageReturnProto.MessageReturn) msg;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
	{
		// 释放资源
		ctx.close();
	}
}
