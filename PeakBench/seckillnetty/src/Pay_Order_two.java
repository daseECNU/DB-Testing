
import org.voltdb.*;
import java.sql.Timestamp;

public class Pay_Order_two extends VoltProcedure
{
	public final SQLStmt updateOrders = new SQLStmt(
			"update orders set o_state = 1,o_paydate = ? where o_orderkey = ?;");
	public final SQLStmt updateSeckillpay = new SQLStmt(
			"update seckillpay set sa_paycount = sa_paycount + 1 where sa_skpkey = ?;");
	public VoltTable[] run(int seckillkey, int orderkey, Timestamp time) throws VoltAbortException 
	{
		voltQueueSQL( updateOrders, time, orderkey);
		voltExecuteSQL();
		voltQueueSQL( updateSeckillpay, seckillkey);
		return voltExecuteSQL();
	}
}
