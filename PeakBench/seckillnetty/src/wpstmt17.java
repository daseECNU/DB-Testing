
import org.voltdb.*;

public class wpstmt17 extends VoltProcedure 
{
	public final SQLStmt select_allorders = new SQLStmt(
			"select * from orders where o_custkey = ? limit 10;");
	public VoltTable[] run(int orderkey) throws VoltAbortException 
	{
		voltQueueSQL( select_allorders, orderkey);
		return voltExecuteSQL();
	}
}