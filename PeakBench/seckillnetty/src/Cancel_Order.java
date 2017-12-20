 import org.voltdb.*;

public class Cancel_Order extends VoltProcedure
{
	public final SQLStmt updateOrders = new SQLStmt(
			"update orders set o_state = 2 where o_orderkey = ?;");
	public final SQLStmt updateSeckillplan = new SQLStmt(
			"update seckillplan set sl_skpcount = sl_skpcount - 1 where sl_skpkey = ?;");
	public VoltTable[] run(int seckillkey, int orderkey) throws VoltAbortException 
	{
		voltQueueSQL( updateOrders, orderkey);
		voltExecuteSQL();
		voltQueueSQL( updateSeckillplan, seckillkey);
		return voltExecuteSQL();
	}
}
