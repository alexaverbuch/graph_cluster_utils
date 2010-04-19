package graph_cluster_utils.simulation;

import graph_cluster_utils.change_log.ChangeOp;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class SimulationCoordinatorBlocking implements Queue<ChangeOp> {

	private Queue<ChangeOp> changeLog = null;

	private boolean ptnAlgReady = true;
	private boolean accessSimReady = false;

	private Thread ptnAlgThread = null;
	private Thread accessSimThread = null;

	public SimulationCoordinatorBlocking(Thread ptnAlgThread,
			Thread accessSimThread) {
		this.ptnAlgReady = true;
		this.accessSimReady = false;

		// Synchronized
		// Blocks when empty
		this.changeLog = new LinkedBlockingQueue<ChangeOp>();

		this.ptnAlgThread = ptnAlgThread;
		this.accessSimThread = accessSimThread;
	}

	@Override
	// NOTE should return "true" or throw "IllegalStateException"
	// NOTE may return "false" to signify "sleep" signal
	public boolean add(ChangeOp e) {
		if (e instanceof ChangeOpEnd) {
			accessSimReady = true;
			if (ptnAlgReady == true)
				ptnAlgThread.interrupt();
		}
		return changeLog.add(e);
	}

	@Override
	public ChangeOp poll() {
		ChangeOp changeOp = changeLog.poll();
		if (changeOp instanceof ChangeOpEnd)
			return null;
		return changeOp;
	}

	@Override
	public boolean isEmpty() {
		return changeLog.isEmpty();
	}

	@Override
	public int size() {
		return changeLog.size();
	}

	// ***************
	// Not implemented
	// ***************

	@Override
	public Iterator<ChangeOp> iterator() {
		// NOTE Not supported
		throw new UnsupportedOperationException();
	}

	@Override
	public ChangeOp element() {
		// NOTE Not supported
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean offer(ChangeOp e) {
		// NOTE Not supported
		throw new UnsupportedOperationException();
	}

	@Override
	public ChangeOp peek() {
		// NOTE Not supported
		throw new UnsupportedOperationException();
	}

	@Override
	public ChangeOp remove() {
		// NOTE Not supported
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(Collection<? extends ChangeOp> c) {
		// NOTE Not supported
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		// NOTE Not supported
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean contains(Object o) {
		// NOTE Not supported
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		// NOTE Not supported
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o) {
		// NOTE Not supported
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		// NOTE Not supported
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		// NOTE Not supported
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] toArray() {
		// NOTE Not supported
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		// NOTE Not supported
		throw new UnsupportedOperationException();
	}

}
