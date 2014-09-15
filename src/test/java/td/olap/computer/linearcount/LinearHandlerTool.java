package td.olap.computer.linearcount;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.clearspring.analytics.stream.cardinality.LinearCounting;

public class LinearHandlerTool extends LinearCounting {

	public LinearHandlerTool(int size) {
		super(size);
	}

	public static LinearHandler merge(LinearHandler... handlers)
			throws MergeNotSupportException {
		LinearHandler target = null;
		List<LinearCounting> estimators = new ArrayList<LinearCounting>();
		for (LinearHandler handler : handlers) {
			estimators.add(handler.getLc());
		}
		try {
			LinearCounting targetLinearCounting = LinearCounting
					.mergeEstimators(estimators
							.toArray(new LinearCounting[] {}));
			target = new LinearHandler(targetLinearCounting.getBytes());
		} catch (LinearCountingMergeException e) {
			throw new MergeNotSupportException(e);
		}
		return target;
	}

	public static void main(String[] args) {
		LinearHandler l1 = new LinearHandler("l1", 12000);
		LinearHandler l2 = new LinearHandler("l1", 12000);
		LinearHandler l3 = new LinearHandler("l1", 12000);
		LinearHandler l4 = new LinearHandler("l1", 12000);
		for (int i = 0; i < 10000; i++) {
			l1.offer(UUID.randomUUID());
			l2.offer(UUID.randomUUID());
			l3.offer(UUID.randomUUID());
			l4.offer(UUID.randomUUID());
		}

		System.out.println("l1 count:" + l1.getCount());
		System.out.println("l2 count:" + l2.getCount());
		System.out.println("l3 count:" + l3.getCount());
		System.out.println("l4 count:" + l4.getCount());

		try {
			System.out.println("merge count:"
					+ LinearHandlerTool.merge(l1, l2, l3, l4).getCount());
		} catch (MergeNotSupportException e) {
			e.printStackTrace();
		}
	}

}