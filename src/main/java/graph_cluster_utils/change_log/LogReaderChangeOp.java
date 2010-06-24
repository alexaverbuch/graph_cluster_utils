package graph_cluster_utils.change_log;

import graph_gen_utils.general.Consts;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Scanner;

public class LogReaderChangeOp {

	private File changeOpLogFile = null;

	public LogReaderChangeOp(File changeOpLogFile) {
		this.changeOpLogFile = changeOpLogFile;
	}

	public Iterable<ChangeOp> getChangeOps() {
		try {
			return new ChangeOpIterator(changeOpLogFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	private class ChangeOpIterator implements Iterator<ChangeOp>,
			Iterable<ChangeOp> {

		private ChangeOp nextChangeOp = null;
		private Scanner changeOpScanner = null;
		private boolean eof = false;

		public ChangeOpIterator(File changeOpLogFile)
				throws FileNotFoundException {
			this.changeOpScanner = new Scanner(changeOpLogFile);
		}

		@Override
		public Iterator<ChangeOp> iterator() {
			return this;
		}

		@Override
		public boolean hasNext() {
			if (nextChangeOp != null)
				return true;

			nextChangeOp = getNextChangeOp();

			// NOTE Old Code
			// return (nextChangeOp != null);

			// nextChangeOp == null && eof == false
			// ---> eof = true
			// ---> return true
			if (nextChangeOp == null && eof == false) {
				eof = true;
				return true;
			}

			// nextChangeOp != null && eof == false
			// ---> return true
			if (nextChangeOp != null && eof == false) {
				return true;
			}

			// nextChangeOp == null && eof == true
			// ---> return false
			if (nextChangeOp == null && eof == true) {
				return false;
			}

			// nextChangeOp != null && eof == true
			// ---> throw Exception
			throw new NoSuchElementException();
		}

		@Override
		public ChangeOp next() {
			if (eof == true)
				return new ChangeOpEnd();

			ChangeOp changeOp = null;

			if (nextChangeOp != null) {
				changeOp = nextChangeOp;
				nextChangeOp = null;
				return changeOp;
			}

			nextChangeOp = getNextChangeOp();

			if (nextChangeOp != null) {
				changeOp = nextChangeOp;
				nextChangeOp = null;
				return changeOp;
			}

			throw new NoSuchElementException();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		private ChangeOp getNextChangeOp() {
			try {
				if (changeOpScanner.hasNextLine() == false)
					return null;

				String[] args = changeOpScanner.nextLine().split(
						"[;\\t\\n\\r\\f]");

				// FIXME Temp
				// for (String arg : args)
				// System.out.printf(arg);
				// System.out.println();

				ChangeOp changeOp = parseChangeOp(args);

				return changeOp;
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}

		}

		private ChangeOp parseChangeOp(String[] args) {
			if (args[0].equals(Consts.CHANGELOG_OP_ADD_NODE)) {
				long id = Long.parseLong(args[1]);
				byte color = Byte.parseByte(args[2]);
				return new ChangeOpAddNode(id, color);
			}

			if (args[0].equals(Consts.CHANGELOG_OP_ADD_RELATIONSIHP)) {
				long id = Long.parseLong(args[1]);
				long startNodeId = Long.parseLong(args[2]);
				long endNodeId = Long.parseLong(args[3]);
				return new ChangeOpAddRelationship(id, startNodeId, endNodeId);
			}

			if (args[0].equals(Consts.CHANGELOG_OP_DELETE_NODE)) {
				long id = Long.parseLong(args[1]);
				return new ChangeOpDeleteNode(id);
			}

			if (args[0].equals(Consts.CHANGELOG_OP_DELETE_RELATIONSHIP)) {
				long id = Long.parseLong(args[1]);
				return new ChangeOpDeleteRelationship(id);
			}

			String errMsg = String
					.format("Not a valid ChangeOp[%s]\n", args[0]);
			throw new NoSuchElementException(errMsg);
		}

	}

}
