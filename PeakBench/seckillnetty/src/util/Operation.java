package util;

import io.netty.channel.ChannelHandlerContext;

public class Operation<T>
{
	public ChannelHandlerContext ctx = null;
	public T message = null;
	public int N = 0;
	public int type = 0;//read1;write2
	public long time = 0;
	public Operation(ChannelHandlerContext ctx, T msg)
	{
		super();
		this.ctx = ctx;
		this.message = msg;
	}
	public Operation(ChannelHandlerContext ctx, T msg, int N, int type, long time)
	{
		super();
		this.ctx = ctx;
		this.message = msg;
		this.N = N;
		this.type = type;
		this.time = time;
	}
	public Operation(T msg, int N, int type, long time)
	{
		super();
		this.message = msg;
		this.N = N;
		this.type = type;
		this.time = time;
	}
	public Operation( T msg)
	{
		super();
		this.message = msg;
	}
}
