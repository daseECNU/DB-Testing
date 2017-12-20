
import org.voltdb.*;

public class wpstmt9 extends VoltProcedure 
{
	public final SQLStmt select_orders = new SQLStmt(
			"select * from orders where o_custkey = ? and o_state = 0;");
	public VoltTable[] run(int customerkey) throws VoltAbortException 
	{
		voltQueueSQL( select_orders, customerkey);
		return voltExecuteSQL();
	}

}
