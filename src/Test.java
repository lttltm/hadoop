import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class Test {
	
	public static void main(String[] args) throws IOException {
		FileInputStream fis = new FileInputStream("data/friends.txt");
		
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);
		
	
		String str = null;
		while((str = br.readLine()) != null )
		{
			String[] friends = str.split("\t");
			
			System.out.println(friends.length);
		}
	}
}
