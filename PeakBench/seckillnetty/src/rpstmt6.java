
import org.voltdb.*;

public class rpstmt6 extends VoltProcedure 
{
	public final SQLStmt select_supplier = new SQLStmt(
			"select * from seckillplan where sl_skpkey = ?;");
	public VoltTable[] run(int seckillkey) throws VoltAbortException 
	{
		voltQueueSQL( select_supplier, seckillkey);
		return voltExecuteSQL();
	}
}