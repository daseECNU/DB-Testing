package com.conflict.onedb.server;
import java.util.concurrent.atomic.AtomicInteger;

import com.conflict.onedb.client.MessageProto;

import util.Operation;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class NettyServerHandler extends ChannelInboundHandlerAdapter 
{
	public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
    	try
		{
    		MessageProto.Message message = (MessageProto.Message) msg;
			if(message.getType()==1)
			{
				Operation<MessageProto.Message> operation = new Operation<MessageProto.Message>(ctx,  message, 1, 1, System.nanoTime());
				ProceseNode.getreadDbOperator().addOperation(operation);
			}
			else if(message.getType()==2)
			{
				if(message.getMessageWrite().getSeckorder() == 1)
				{
					Operation<MessageProto.Message> operation = new Operation<MessageProto.Message>(ctx,  message, 
							message.getConflict(), 2, System.nanoTime());
					ProceseNode.getDbOperator(message.getMessageWrite().getSlskpID()).addOperation(operation);
				}
				else
				{
					Operation<MessageProto.Message> operation = new Operation<MessageProto.Message>(ctx,  message, 
							1, 2, System.nanoTime());
					ProceseNode.getDbOperator().addOperation(operation);
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) 
    {
		cause.printStackTrace();
		ctx.close();
    }
}
