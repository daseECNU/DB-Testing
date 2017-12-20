
import org.voltdb.*;
import java.sql.Timestamp;

public class Pay_Order_one extends VoltProcedure
{
	public final SQLStmt updateOrders = new SQLStmt(
			"update orders set o_state = 1,o_paydate = ? where o_orderkey = ?;");
	public final SQLStmt updateSeckillplan = new SQLStmt(
			"update seckillplan set sl_paycount = sl_paycount + 1 where sl_skpkey = ?;");
	public VoltTable[] run(int seckillkey, int orderkey, Timestamp time) throws VoltAbortException 
	{
		voltQueueSQL( updateOrders, time, orderkey);
		voltExecuteSQL();
		voltQueueSQL( updateSeckillplan, seckillkey);
		return voltExecuteSQL();
	}
}
