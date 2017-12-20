
import org.voltdb.*;

public class wpstmt10 extends VoltProcedure 
{
	public final SQLStmt select_ordersprice = new SQLStmt(
			"select o_price from orders where o_orderkey = ?;");
	public VoltTable[] run(int orderkey) throws VoltAbortException 
	{
		voltQueueSQL( select_ordersprice, orderkey);
		return voltExecuteSQL();
	}
}
