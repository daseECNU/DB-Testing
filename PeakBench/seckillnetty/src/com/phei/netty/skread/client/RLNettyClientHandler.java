package com.phei.netty.skread.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class RLNettyClientHandler extends ChannelInboundHandlerAdapter
{
	public RLNettyClientHandler(){
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx){
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
	{
//		MessageReadProto.MessageRead message = (MessageReadProto.MessageRead) msg;
//		if(message.getType() == 1)
//		{
//			
//		}
//		else if(message.getType() == 2)
//		{
//			
//		}
//		else if(message.getType() == 3)
//		{
//			
//		}
//		else if(message.getType() == 4)
//		{
//			
//		}
//		else if(message.getType() == 5)
//		{
//			
//		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
	{
		// 释放资源
		ctx.close();
	}
}
