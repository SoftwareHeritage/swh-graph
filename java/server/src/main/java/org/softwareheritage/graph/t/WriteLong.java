import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class WriteLong {
    public static void main(String args[]) {
	String filename = null;
	try {
	    filename = args[0];
	    FileOutputStream file = new FileOutputStream(filename);
	    DataOutputStream data = new DataOutputStream(file);
	    while (true) {
		data.writeLong(Long.parseLong(args[1]));
	    }
	    //data.close();
	} catch (IOException e) {
	    System.out.println("cannot write to file " + filename + "\n" + e);
	    System.exit(2);
	} catch (ArrayIndexOutOfBoundsException e) {
	    System.out.println("Usage: Writer FILENAME INT");
	    System.exit(1);
	}
    }
}
