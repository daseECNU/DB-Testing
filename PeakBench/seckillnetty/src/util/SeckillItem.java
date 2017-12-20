package util;
public class SeckillItem implements Comparable<SeckillItem>{
	public int skpkey;
	public int itemkey;
	public int plancount;

	public SeckillItem()
	{
		super();
	}
	public SeckillItem(int skpkey, int itemkey, int plancount) {
		super();
		this.skpkey = skpkey;
		this.itemkey = itemkey;
		this.plancount = plancount;
	}

	public int compareTo(SeckillItem s) {
		if(this.plancount > s.plancount) {
			return -1;
		} else if(this.plancount < s.plancount) {
			return 1;
		} else {
			return 0;
		}
	}
}