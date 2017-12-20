package util;

public class otherTask
{
	private int seckflog  = 0;
	private int orderid = 0;
	private int customid = 0;
	private int slskpid  = 0;
	private int type  = 0;
	public int getType()
	{
		return type;
	}
	public int getSeckflog()
	{
		return seckflog;
	}
	public int getOrderid()
	{
		return orderid;
	}
	public int getCustomid()
	{
		return customid;
	}
	public int getSlskpid()
	{
		return slskpid;
	}
	public otherTask(int seckflog, int orderid, int customid, int slskpid, int type)
	{
		this.seckflog = seckflog;
		this.orderid = orderid;
		this.customid = customid;
		this.slskpid = slskpid;
		this.type = type;
	}
	public otherTask(int seckflog, int orderid, int customid, int type)
	{
		this.seckflog = seckflog;
		this.orderid = orderid;
		this.customid = customid;
		this.type = type;
	}
}
