package com.phei.netty.skread.server;

import util.Operation;
import com.phei.netty.skread.client.MessageReadProto;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * @author lilinfeng
 * @date 2014年2月14日
 * @version 1.0
 */
public class RLNettyServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
//    	System.out.println("RLnettyserver message~~~~~~~~~~");
    	try
		{
    		MessageReadProto.MessageRead message = (MessageReadProto.MessageRead) msg;
			Operation<MessageReadProto.MessageRead> operation = new Operation<MessageReadProto.MessageRead>(ctx,  message);
			RLProceseNode.getDbOperator().addOperation(operation);
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
