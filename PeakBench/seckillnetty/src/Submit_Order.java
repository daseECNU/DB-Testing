
import java.sql.Timestamp;
import org.voltdb.*;

public class Submit_Order extends VoltProcedure
{
	public final SQLStmt updateSeckillplan = new SQLStmt(
			"update seckillplan set sl_skpcount = sl_skpcount + 1 where sl_skpkey = ? "
			+ "and sl_skpcount < sl_plancount;");	
	public final SQLStmt insertOrders = new SQLStmt(
			"insert into orders (o_orderkey, o_custkey, o_skpkey, o_price, o_orderdate, o_state) "
			+ "values (?, ?, ?, ?, ?, 0);");
	public final SQLStmt insertOrderitem = new SQLStmt(
			"insert into orderitem (o_orderkey, oi_itemkey, oi_count, oi_price) values "
			+ "(?, ?, 1, ?);");
	public VoltTable[] run(int seckillkey, int orderkey, int costomkey, int itemkey, double price, Timestamp time) 
			throws VoltAbortException {
		voltQueueSQL( updateSeckillplan, seckillkey);
		VoltTable[] row = voltExecuteSQL();	
		if(row[0].fetchRow(0).getLong(0)>0)
		{
			voltQueueSQL( insertOrders, orderkey, costomkey, seckillkey, price, time);
			voltExecuteSQL();
			voltQueueSQL( insertOrderitem, orderkey, itemkey, price);
			return voltExecuteSQL();	
		}
		else
		{
			return row;	
		}
	}
}
