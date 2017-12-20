package com.phei.netty.skread.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class RLReturnNettyClienthandler extends ChannelInboundHandlerAdapter
{
	public RLReturnNettyClienthandler()	{
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx){
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
	{
//		MessageReadReturnProto.MessageReadReturn message = (MessageReadReturnProto.MessageReadReturn) msg;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
	{
		// 释放资源
		ctx.close();
	}

}
