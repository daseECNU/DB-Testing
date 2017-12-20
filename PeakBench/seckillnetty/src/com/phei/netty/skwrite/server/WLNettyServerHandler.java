package com.phei.netty.skwrite.server;

import util.Operation;
import com.phei.netty.skwrite.client.MessageWriteProto;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class WLNettyServerHandler extends ChannelInboundHandlerAdapter
{
	  public void channelRead(ChannelHandlerContext ctx, Object msg)
	    {
//	    	System.out.println("RLnettyserver message~~~~~~~~~~");
	    	try
			{
	    		MessageWriteProto.MessageWrite message = (MessageWriteProto.MessageWrite) msg;
				Operation<MessageWriteProto.MessageWrite> operation = new Operation<MessageWriteProto.MessageWrite>(ctx,  message);
				if(operation.message.getSeckorder() == 1)
				{
					WLProceseNode.getDbOperator(message.getSlskpID()).addOperation(operation);
				}
				else
				{
					WLProceseNode.getDbOperator().addOperation(operation);
				}
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
