
import org.voltdb.*;

public class wpstmt18 extends VoltProcedure 
{
	public final SQLStmt select_orders = new SQLStmt(
			"select * from orders where o_orderkey = ?;");
	public VoltTable[] run(int orderkey) throws VoltAbortException 
	{
		voltQueueSQL( select_orders, orderkey);
		return voltExecuteSQL();
	}
}