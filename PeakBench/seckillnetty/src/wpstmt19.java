
import org.voltdb.*;

public class wpstmt19 extends VoltProcedure 
{
	public final SQLStmt select_orderitem = new SQLStmt(
			"select * from orderitem where oi_orderkey = ?;");
	public VoltTable[] run(int orderkey) throws VoltAbortException 
	{
		voltQueueSQL( select_orderitem, orderkey);
		return voltExecuteSQL();
	}
}