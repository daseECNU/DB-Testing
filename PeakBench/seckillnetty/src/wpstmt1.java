
import org.voltdb.*;

public class wpstmt1 extends VoltProcedure
{
	public final SQLStmt select_planprice = new SQLStmt(
			"select sl_price from seckillplan where sl_skpkey = ?;");
	public VoltTable[] run(int seckillkey) throws VoltAbortException 
	{
		voltQueueSQL( select_planprice, seckillkey);
		return voltExecuteSQL();
	}
}
