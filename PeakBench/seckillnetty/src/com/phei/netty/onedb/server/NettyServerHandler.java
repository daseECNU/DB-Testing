package com.phei.netty.onedb.server;

import util.Operation;
import com.phei.netty.onedb.client.MessageProto;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class NettyServerHandler extends ChannelInboundHandlerAdapter 
{
	public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
//    	System.out.println("RLnettyserver message~~~~~~~~~~");
    	try
		{
    		MessageProto.Message message = (MessageProto.Message) msg;
			Operation<MessageProto.Message> operation = new Operation<MessageProto.Message>(ctx,  message);
			if(message.getType()==1)
			{
				ProceseNode.getDbOperator().addOperation(operation);
			}
			else if(message.getType()==2)
			{
				if(message.getMessageWrite().getSeckorder() == 1)
				{
					ProceseNode.getDbOperator(message.getMessageWrite().getSlskpID()).addOperation(operation);
				}
				else
				{
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
