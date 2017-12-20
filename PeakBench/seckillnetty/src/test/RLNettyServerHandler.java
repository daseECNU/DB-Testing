package test;

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
    	System.out.println("RLnettyserver message~~~~~~~~~~");
    	try
		{
//			Operation operation = new Operation(ctx, msg);
//			ReadLoadProceseNode.getDbOperator().addOperation(operation);
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
