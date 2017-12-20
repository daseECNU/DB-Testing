package com.conflict.onedb.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class NettyClientHandler extends ChannelInboundHandlerAdapter
{
	public NettyClientHandler(){
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx){
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
	{
		
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
	{
		// 释放资源
		ctx.close();
	}
}
