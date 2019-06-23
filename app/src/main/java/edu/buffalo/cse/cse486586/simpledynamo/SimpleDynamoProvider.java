package edu.buffalo.cse.cse486586.simpledynamo;


import java.io.*;
import java.lang.reflect.Array;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static HashSet<String> myFiles= new HashSet<String>();
	static final int SERVER_PORT = 10000;
	static String myNextNextPort="";
	static String myNextPort="";
	static String myPrevPort="";
	static String myPortHash="";
	static String myPrevPrev="";
	static ArrayList<String> originalPorts= new ArrayList<String>();
	HashMap<String,String> originalHash= new HashMap<String, String>();
	static Boolean min=false;
	static ArrayList<String> ports=new ArrayList<String>();
	static HashMap<String,ArrayList<String>> allPorts= new HashMap<String,ArrayList<String>>();
	static String myPortNumber="";
	//static HashMap<String, String> replicationStatus= new HashMap<String, String>();
	static HashMap<String, ArrayList<String>> messageRetrieval= new  HashMap<String, ArrayList<String>>();
	static HashMap<String,Integer> messageCounter= new HashMap<String,Integer>();



	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(selection.contains("@") || selection.contains("*")){
            File[] files  = getContext().getFilesDir().listFiles();
            for(File file:files){
                file.delete();
            }
            messageRetrieval=null;
        }
        else{
            try {
                //  Log.e("delete"," testing else");
                String keyHash = genHash(selection);
                for (int i = 0; i < ports.size(); i++) {

                    if ((i == ports.size() - 1 && keyHash.compareTo(ports.get(i)) > 0) || (i == 0 && keyHash.compareTo(ports.get(i)) <= 0)) {
                        Node n = new Node();
                        n.setCurrentPort(ports.get(0));
                        n.setMessage(selection);
                        n.setOperationType("delete");
                        n.setNextPort(allPorts.get(ports.get(0)).get(1));
                        n.setNextNextPort(allPorts.get(ports.get(0)).get(2));
                        //Log.e("delete if :",n.getMessage());
                        Client c = new Client(n);
                        c.start();
                        c.deleteQuery();



                    } else if (keyHash.compareTo(ports.get(i)) > 0 && keyHash.compareTo(ports.get(i + 1)) <= 0) {
                        Node n = new Node();
                        n.setCurrentPort(ports.get(i+1));
                        n.setMessage(selection);
                        n.setOperationType("delete");
                        n.setNextPort(allPorts.get(ports.get(i+1)).get(1));
                        n.setNextNextPort(allPorts.get(ports.get(i+1)).get(2));
                        //Log.e("delete elseif :",n.getMessage());
                        Client c = new Client(n);
                        c.start();
                        c.deleteQuery();
                    } else {
                        continue;

                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            }

        return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        String msg_key = values.getAsString("key");
        String msg_value = values.getAsString("value");
        Context con = getContext();

        try {

            String keyHash = genHash(msg_key);
//            if(myPortHash.compareTo(myPrevPort)<0 && myPortHash.compareTo(myNextPort)<0 && !min){
//                min=true;
//            }


                for(int i=0;i< ports.size();i++){

                    if((i==ports.size()-1 && keyHash.compareTo(ports.get(i))>0) ||(i==0 && keyHash.compareTo(ports.get(i))<=0)){

                        Node node= new Node();
                        node.setOperationType("readVersionsForPropagatedInsertion");
                        node.setMessage(msg_key);
                        node.setNextPort(allPorts.get(ports.get(0)).get(1));
                        node.setNextNextPort(allPorts.get(ports.get(0)).get(2));
                        node.setCurrentPort(ports.get(0));
                        Client c = new Client(node);
                        c.start();
                        String newVersion=(Integer.parseInt(c.readVersionsForInsertion(node))+1)+"";
                        node.setVersion(newVersion);
                        messageCounter.put(node.getMessage(),Integer.parseInt(newVersion));
                        node.setValue(msg_value);
                        node.setOperationType("writeVersionsForPropagatedReplication");
                        int replicated=c.writeMessageForReplicas(node);




                    }
                    else if(keyHash.compareTo(ports.get(i))>0 && keyHash.compareTo(ports.get(i+1))<=0){


                        Node node= new Node();
                        node.setOperationType("readVersionsForPropagatedInsertion");
                        node.setMessage(msg_key);
                        node.setNextPort(allPorts.get(ports.get(i+1)).get(1));
                        node.setNextNextPort(allPorts.get(ports.get(i+1)).get(2));
                        node.setCurrentPort(ports.get(i+1));
                        Client c = new Client(node);
                        c.start();
                        String newVersion=(Integer.parseInt(c.readVersionsForInsertion(node))+1)+"";
                        node.setVersion(newVersion);

                        messageCounter.put(node.getMessage(),Integer.parseInt(newVersion));
                        node.setValue(msg_value);
                        node.setOperationType("writeVersionsForPropagatedReplication");
                        c.writeMessageForReplicas(node);

                    }
                    else{
                        continue;

                    }

            }

        } catch (Exception e) {
            Log.e("exception", "");
        }
        return uri;
	}

	@Override
	public boolean onCreate() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		myPortNumber = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);//5554

		Server sarvam= new Server();
		sarvam.start();
		Log.e("My port",myPortNumber);
		originalPorts.add("5554");
		originalPorts.add("5556");
		originalPorts.add("5558");
		originalPorts.add("5560");
		originalPorts.add("5562");
		try {
			myPortHash=genHash(myPortNumber);
			for( String s:originalPorts){
				String portHash=genHash(s);
				ports.add(portHash);
				originalHash.put(portHash,s);
			}

			Collections.sort(ports);
			for(int i=0;i<ports.size();i++){
				//if(myPortHash.compareTo(ports.get(i))==0){
				if(i==0){
					myPrevPort=ports.get(ports.size()-1);
					myNextPort=ports.get(i+1);
					myNextNextPort=ports.get(i+2);

					ArrayList<String> as = new ArrayList<String>();
					as.add(myPrevPort);
					as.add(myNextPort);
					as.add(myNextNextPort);
					allPorts.put(ports.get(i),as);

				}
				else if(i==ports.size()-2){
					myPrevPort=ports.get(i-1);
					myNextPort=ports.get(i+1);
					myNextNextPort=ports.get(0);

					ArrayList<String> as = new ArrayList<String>();
					as.add(myPrevPort);
					as.add(myNextPort);
					as.add(myNextNextPort);
					allPorts.put(ports.get(i),as);
				}
                else if(i==ports.size()-1){
                    myPrevPort=ports.get(i-1);
                    myNextPort=ports.get(0);
                    myNextNextPort=ports.get(1);

                    ArrayList<String> as = new ArrayList<String>();
                    as.add(myPrevPort);
                    as.add(myNextPort);
                    as.add(myNextNextPort);
                    allPorts.put(ports.get(i),as);

                }
				else{
					myPrevPort=ports.get(i-1);
					myNextPort=ports.get(i+1);
					myNextNextPort=ports.get(i+2);

					ArrayList<String> as = new ArrayList<String>();
					as.add(myPrevPort);
					as.add(myNextPort);
					as.add(myNextNextPort);
					allPorts.put(ports.get(i),as);
				}
			}
			//}
			myPrevPort=allPorts.get(myPortHash).get(0);
			myNextPort=allPorts.get(myPortHash).get(1);
			myNextNextPort=allPorts.get(myPortHash).get(2);
            myPrevPrev=allPorts.get(myPrevPort).get(0);

            Log.e("Prev: ",originalHash.get(myPrevPort));
            Log.e("Next: ",originalHash.get(myNextPort));
            Log.e("Next Next: ",originalHash.get(myNextNextPort));
            Log.e("Prev Prev: ",originalHash.get(myPrevPrev));
            Node node= new Node();
            node.setOperationType("failureHandling");
            node.setNextPort(myNextPort);
            node.setPrevPort(myPrevPort);
            node.setCurrentPort(myPortHash);
            node.setNextNextPort(myNextNextPort);
            Client c= new Client(node);
            c.start();

		}
		catch (Exception e){
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

        String write_fields[] = {"key", "value"};
        MatrixCursor mc = new MatrixCursor(write_fields);
        Context context_obj = getContext();
        //String reqPort = portStr;

        if (selection.equals("*") || selection.equals("@")) {

            for (String s : myFiles) {
                try {
                    //using FileInputStream class
                    FileInputStream fis = context_obj.openFileInput(s);
                    InputStreamReader inputStreamReader = new InputStreamReader(fis);
                    String retrieved_values = new BufferedReader(inputStreamReader).readLine();
                    String arr_content[] = {s, retrieved_values};
                    mc.addRow(arr_content);
                    fis.close();


                    // return mc;
                } catch (IOException e) {

                    //Log.e(TAG,e.getMessage());
                    Log.e("Qu", "Querying failed");
                }
            }
            if(selection.equals("*")){
               // Log.e("Query:entered: ", "*");
                for(String s:originalPorts){
                    if(!(s).equals(myPortNumber) ){
                        Log.e("Query:ports", s);
                        Node node = new Node();
                        node.setOperationType("all");
                        node.setCurrentPort(s);
                        node.setValue(selection);
                        Log.e("Called",s);
                        Client c1= new Client(node);
                        c1.start();
                        ArrayList<String> ast=c1.fromAllPorts();
                       // Log.e("Size in query",ast.size()+"");
                        if(ast!=null) {
                            for (String b : ast) {
                                String arr_content[] = {b.split(",")[0], b.split(",")[1]};
                                mc.addRow(arr_content);
                            }
                        }
                    }
                }
            }


        }
        else {
                    try {
                        String keyHash = genHash(selection);
                        for (int i = 0; i < ports.size(); i++) {

                            if ((i == ports.size() - 1 && keyHash.compareTo(ports.get(i)) > 0) || (i == 0 && keyHash.compareTo(ports.get(i)) <= 0)) {


                                Node n = new Node();
                                n.setCurrentPort(ports.get(0));
                                n.setMessage(selection);
                                n.setOperationType("query");
                                n.setNextPort(allPorts.get(ports.get(0)).get(1));
                                n.setNextNextPort(allPorts.get(ports.get(0)).get(2));
                                //Log.e("query, if", n.getMessage());
                                Client c = new Client(n);
                                c.start();
                                String answer[] = c.fromOnePort();
                                mc.addRow(answer);
                                return mc;


                            } else if (keyHash.compareTo(ports.get(i)) > 0 && keyHash.compareTo(ports.get(i + 1)) <= 0) {

                                Node n = new Node();
                                n.setCurrentPort(ports.get(i + 1));
                                n.setOperationType("query");
                                n.setMessage(selection);
                                n.setNextPort(allPorts.get(ports.get(i+1)).get(1));
                                n.setNextNextPort(allPorts.get(ports.get(i+1)).get(2));
                               // Log.e("query, else if", n.getMessage());
                                Client c = new Client(n);
                                c.start();

                                String answer[] = c.fromOnePort();
                                mc.addRow(answer);
                                return mc;

                            } else {
                                continue;

                            }
                        }


                        //}

                    } catch (Exception e) {
                        e.printStackTrace();
                    }



        }
        return mc;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	public class Server extends Thread {
		//private Uri AUri = null;
		Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
		public void run() {
			Socket client= new Socket();
			try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

				while (true) {

                    client = serverSocket.accept();
                    DataInputStream ois = new DataInputStream(new BufferedInputStream(client.getInputStream()));
                    String n = ois.readUTF();

                    if (n.contains("readVersions")) {
                       String[] splits = n.split(",");
                        if (!messageCounter.containsKey(splits[1])) {
                            messageCounter.put(splits[1], 0);
                        }
                        DataOutputStream dos = new DataOutputStream(client.getOutputStream());

                        dos.writeUTF(messageCounter.get(splits[1]) + "");
                    } else if (n.contains("writeVersions")) {

                        Context con = getContext();
                        String[] splits = n.split(",");
                        FileOutputStream out = con.openFileOutput(splits[1], Context.MODE_PRIVATE);
                        out.write(splits[2].getBytes());
                        out.close();
                        messageCounter.put(splits[1], Integer.parseInt(splits[3]));

                        ArrayList<String> as = new ArrayList<String>();
                        //actual Port,value,version
                        as.add(splits[4]);
                        as.add(splits[2]);
                        as.add(splits[3]);
                        messageRetrieval.put(splits[1],as);
                        Log.e("Server","insert"+','+splits[1]+","+splits[2]+","+originalHash.get(splits[4]));
                        DataOutputStream dos = new DataOutputStream(client.getOutputStream());
                        dos.writeUTF("replicated");
                        myFiles.add(splits[1]);

                    } else if (n.equals("all")) {
                        DataOutputStream dos = new DataOutputStream(client.getOutputStream());
                        String[] proj = {"key", "value"};
                        Cursor q = query(mUri, proj, "@", null, "");
                        String foundall = "";
                        if (q.getCount() >= 1) {

                            while (q.moveToNext()) {
                                String key = q.getString(0);
                                String value = q.getString(1);
                                String keyValue = key + "&" + value;
                                foundall = foundall + keyValue + ",";
                            }
                        }
                        dos.writeUTF(foundall);
                    } else if (n.contains("query")) {
                        String[] splits = n.split(",");
                        FileInputStream fis = getContext().openFileInput(splits[1]);
                        InputStreamReader inputStreamReader = new InputStreamReader(fis);
                        String retrieved_values = new BufferedReader(inputStreamReader).readLine();

                        DataOutputStream dos = new DataOutputStream(client.getOutputStream());
                        String version = messageCounter.get(splits[1]) + "";
                        Log.e("Server","query"+','+splits[1]+","+retrieved_values);
                        dos.writeUTF(splits[1] + "," + retrieved_values + "," + version);

                    } else if (n.contains("delete")) {
                        String[] splits = n.split(",");
                        File dir = getContext().getFilesDir();
                        File f = new File(dir, splits[1]);
                        //Log.e("server, delete", splits[1]);
                        if (f.exists()) {
                            f.delete();
                            if(messageRetrieval.containsKey(splits[1])) {
                                messageRetrieval.remove(splits[1]);
                            }
                           // Log.e("server, f del", splits[1]);

                        }
                        DataOutputStream dos = new DataOutputStream(client.getOutputStream());
                        dos.writeUTF("deleted");
                    }else if(n.contains("failureHandling")){
                        String[] splits = n.split(",");
                        String reqPort=splits[1];
                        String previousPort="";
                        Log.e("Server","failureHandling");

                        if(messageRetrieval!=null) {
                            if (myNextPort.equals(reqPort)) {
                                Log.e("myNextPort", originalHash.get(myNextPort));
                                Log.e("reqPort", originalHash.get(reqPort));
                                for (Map.Entry<String, ArrayList<String>> dummyMap : messageRetrieval.entrySet()) {
                                    ArrayList<String> ap = dummyMap.getValue();
                                    if (ap.get(0).equals(myPrevPort) || ap.get(0).equals(myPortHash)) {
                                        String s = ap.get(0) + "," + ap.get(1) + "," + ap.get(2) + "," + dummyMap.getKey();
                                        //  Log.e("failureHandling",s);
                                        previousPort = previousPort + s + "&";
                                    }

                                }

                            } else if (myPrevPort.equals(reqPort)) {
                                Log.e("my Prev Port", originalHash.get(myPrevPort));
                                Log.e("reqPort", originalHash.get(reqPort));
                                for (Map.Entry<String, ArrayList<String>> dummyMap : messageRetrieval.entrySet()) {
                                    ArrayList<String> ap = dummyMap.getValue();
                                    if (ap.get(0).equals(myPrevPort)) {
                                        String s = ap.get(0) + "," + ap.get(1) + "," + ap.get(2) + "," + dummyMap.getKey();
                                        // Log.e("failureHandling",s);
                                        previousPort = previousPort + s + "&";
                                    }

                                }
                         }else if(myPrevPrev.equals(reqPort)){
                                for (Map.Entry<String, ArrayList<String>> dummyMap : messageRetrieval.entrySet()) {
                                    ArrayList<String> ap = dummyMap.getValue();
                                    if (ap.get(0).equals(reqPort)) {
                                        String s = ap.get(0) + "," + ap.get(1) + "," + ap.get(2) + "," + dummyMap.getKey();
                                        // Log.e("failureHandling",s);
                                        previousPort = previousPort + s + "&";
                                    }

                                }
                            }

                        }
                        DataOutputStream dos = new DataOutputStream(client.getOutputStream());

                        dos.writeUTF(previousPort);

                    }else if (n.contains("part2")){
                        String[] splits = n.split(",");
                        String reqPort=splits[1];
                        String previousPort="";
                        if(myPrevPort.equals(reqPort)){
                            for (Map.Entry<String, ArrayList<String>> dummyMap : messageRetrieval.entrySet()) {
                                ArrayList<String> ap = dummyMap.getValue();
                                if (ap.get(0).equals(reqPort) || ap.get(0).equals(myPrevPrev)) {
                                    String s = ap.get(0) + "," + ap.get(1) + "," + ap.get(2) + "," + dummyMap.getKey();
                                    // Log.e("failureHandling",s);
                                    previousPort = previousPort + s + "&";
                                }

                            }
                        }else if(myNextNextPort.equals(reqPort)){
                            for (Map.Entry<String, ArrayList<String>> dummyMap : messageRetrieval.entrySet()) {
                                ArrayList<String> ap = dummyMap.getValue();
                                if (ap.get(0).equals(myPortHash)) {
                                    String s = ap.get(0) + "," + ap.get(1) + "," + ap.get(2) + "," + dummyMap.getKey();
                                    // Log.e("failureHandling",s);
                                    previousPort = previousPort + s + "&";
                                }

                            }
                        }

                    }
//                    else if (n.contains("failureHandlingSucc")){
//                        String[] splits = n.split(",");
//                        String reqPort=splits[1];
//                        String previousPort="";
//                        if(myPrevPrev.equals(reqPort)){
//                            for (Map.Entry<String, ArrayList<String>> dummyMap : messageRetrieval.entrySet()) {
//                                ArrayList<String> ap = dummyMap.getValue();
//                                if (ap.get(0).equals(reqPort)) {
//                                    String s = ap.get(0) + "," + ap.get(1) + "," + ap.get(2) + "," + dummyMap.getKey();
//                                    // Log.e("failureHandling",s);
//                                    previousPort = previousPort + s + "&";
//                                }
//
//                            }
//                        }
//                    }
                }
			} catch (Exception e) {
				e.printStackTrace();

			}
			finally {
				try{
					client.close();
				}
				catch (Exception e){
					e.printStackTrace();
				}
			}
		}
	}


	public class Client extends Thread {
        Node node;

        public Client(Node node) {
            this.node = node;
        }

        public void run() {
            try {
            if (node.getOperationType().equals("failureHandling")) {
                Log.e("Client","failure handling");
                Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getNextPort())));
                Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getPrevPort())));
                //Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getNextNextPort())));

                DataOutputStream oos1 = new DataOutputStream(socket1.getOutputStream());
                DataOutputStream oos2 = new DataOutputStream(socket2.getOutputStream());
                //DataOutputStream oos3 = new DataOutputStream(socket3.getOutputStream());

                oos1.writeUTF(node.getOperationType() + "," + node.getCurrentPort());
                oos2.writeUTF(node.getOperationType() + "," + node.getCurrentPort());
                //oos3.writeUTF(node.getOperationType() + "," + node.getCurrentPort());
                DataInputStream ois1 = new DataInputStream(new BufferedInputStream(socket1.getInputStream()));
                DataInputStream ois2 = new DataInputStream(new BufferedInputStream(socket2.getInputStream()));
                try {
                    String n1= ois1.readUTF();
                    if(!n1.equals("")) {

                        String[] splits1 =n1.split("&");

                        for(int i=0;i<splits1.length;i++){
                            String splits[] = splits1[i].split(",");
                            String actualPort = splits[0];
                            String value = splits[1];
                            String version = splits[2];
                            String key = splits[3];
                            FileOutputStream out = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                            out.write(value.getBytes());
                            out.close();
                            messageCounter.put(key, Integer.parseInt(version));
                            ArrayList<String> as = new ArrayList<String>();
                            //actual Port,value,version
                            as.add(actualPort);
                            as.add(value);
                            as.add(version);
                            messageRetrieval.put(key, as);
                            myFiles.add(key);

                        }
                    }
                    socket1.close();
                } catch (Exception e) {

                    //socket1.close();
                    Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(myNextNextPort)));

                    DataOutputStream oos3= new DataOutputStream(socket3.getOutputStream());

                    oos3.writeUTF(node.getOperationType() + "," + node.getCurrentPort());
                     DataInputStream ois3= new DataInputStream(new BufferedInputStream(socket3.getInputStream()));
                    try{

                        String n3= ois3.readUTF();
                        if(!n3.equals("")) {
                            String[] splits1 =n3.split("&");

                            for(int i=0;i<splits1.length;i++){
                                String splits[] = splits1[i].split(",");
                                String actualPort = splits[0];
                                String value = splits[1];
                                String version = splits[2];
                                String key = splits[3];
                                FileOutputStream out = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                                out.write(value.getBytes());
                                out.close();
                                messageCounter.put(key, Integer.parseInt(version));
                                ArrayList<String> as = new ArrayList<String>();
                                //actual Port,value,version
                                as.add(actualPort);
                                as.add(value);
                                as.add(version);
                                messageRetrieval.put(key, as);
                                myFiles.add(key);
                                Log.e("failure",key+","+value);

                            }
                        }
                        socket3.close();
                    }catch(Exception e1){
                        socket3.close();
                    }
                }
                try {
                    String n2= ois2.readUTF();
                    if(!n2.equals("")) {
                        String[] splits1 =n2.split("&");

                        for(int i=0;i<splits1.length;i++){
                            String splits[] = splits1[i].split(",");
                            String actualPort = splits[0];
                            String value = splits[1];
                            String version = splits[2];
                            String key = splits[3];
                            FileOutputStream out = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                            out.write(value.getBytes());
                            out.close();
                            messageCounter.put(key, Integer.parseInt(version));
                            ArrayList<String> as = new ArrayList<String>();
                            //actual Port,value,version
                            as.add(actualPort);
                            as.add(value);
                            as.add(version);
                            messageRetrieval.put(key, as);
                            myFiles.add(key);
                            Log.e("failure",key+","+value);

                        }
                    }
                    socket2.close();

                } catch (Exception e) {
                    //socket2.close();

                    Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getNextPort())));
                    Socket socket4= new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(myPrevPrev)));
                    DataOutputStream oos3 = new DataOutputStream(socket3.getOutputStream());
                    DataOutputStream oos4 = new DataOutputStream(socket4.getOutputStream());
                    node.setOperationType("part2");
                    oos3.writeUTF(node.getOperationType() + "," + node.getCurrentPort());
                    oos4.writeUTF(node.getOperationType() + "," + node.getCurrentPort());
                    //oos3.writeUTF(node.getOperationType() + "," + node.getCurrentPort());
                    DataInputStream ois3 = new DataInputStream(new BufferedInputStream(socket3.getInputStream()));
                    DataInputStream ois4 = new DataInputStream(new BufferedInputStream(socket4.getInputStream()));
                    try{

                        String n3= ois3.readUTF();
                        if(!n3.equals("")) {
                            String[] splits1 =n3.split("&");

                            for(int i=0;i<splits1.length;i++){
                                String splits[] = splits1[i].split(",");
                                String actualPort = splits[0];
                                String value = splits[1];
                                String version = splits[2];
                                String key = splits[3];
                                FileOutputStream out = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                                out.write(value.getBytes());
                                out.close();
                                messageCounter.put(key, Integer.parseInt(version));
                                ArrayList<String> as = new ArrayList<String>();
                                //actual Port,value,version
                                as.add(actualPort);
                                as.add(value);
                                as.add(version);
                                messageRetrieval.put(key, as);
                                myFiles.add(key);
                                Log.e("failure",key+","+value);

                            }
                        }
                        socket3.close();
                    }catch(Exception e1){
                            socket3.close();
                    }
                    try{
                        String n4= ois4.readUTF();
                        if(!n4.equals("")) {
                            String[] splits1 =n4.split("&");

                            for(int i=0;i<splits1.length;i++){
                                String splits[] = splits1[i].split(",");
                                String actualPort = splits[0];
                                String value = splits[1];
                                String version = splits[2];
                                String key = splits[3];
                                FileOutputStream out = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                                out.write(value.getBytes());
                                out.close();
                                messageCounter.put(key, Integer.parseInt(version));
                                ArrayList<String> as = new ArrayList<String>();
                                //actual Port,value,version
                                as.add(actualPort);
                                as.add(value);
                                as.add(version);
                                messageRetrieval.put(key, as);
                                myFiles.add(key);
                                Log.e("failure",key+","+value);

                            }
                        }
                        socket4.close();
                    }catch(Exception e2){
                        socket4.close();
                    }

                }


            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        }

        public String readVersionsForInsertion(Node node) {
            try {
                if (node.getOperationType().equals("readVersionsForPropagatedInsertion")) {

                    Log.e("Client","read versions"+"," +node.getMessage());
                    Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getNextPort())));
                    Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getNextNextPort())));
                    Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getCurrentPort())));

                    DataOutputStream oos1 = new DataOutputStream(socket1.getOutputStream());
                    DataOutputStream oos2 = new DataOutputStream(socket2.getOutputStream());
                    DataOutputStream oos3 = new DataOutputStream(socket3.getOutputStream());
                    oos1.writeUTF(node.getOperationType() + "," + node.getMessage());
                    oos2.writeUTF(node.getOperationType() + "," + node.getMessage());
                    oos3.writeUTF(node.getOperationType() + "," + node.getMessage());
                    DataInputStream ois1 = new DataInputStream(new BufferedInputStream(socket1.getInputStream()));
                    DataInputStream ois2 = new DataInputStream(new BufferedInputStream(socket2.getInputStream()));
                    DataInputStream ois3 = new DataInputStream(new BufferedInputStream(socket3.getInputStream()));
                    int v1 = 0, v2 = 0, v3 = 0, highest = 0;
                    try {
                        String n1 = ois1.readUTF();
                        v1 = Integer.parseInt(n1);
                        socket1.close();
                    } catch (Exception e) {
                        socket1.close();
                    }
                    try {
                        String n2 = ois2.readUTF();
                        v2 = Integer.parseInt(n2);
                        socket2.close();
                    } catch (Exception e) {
                        socket2.close();
                    }
                    try {
                        String n3 = ois3.readUTF();
                        v3 = Integer.parseInt(n3);
                        socket3.close();
                    } catch (Exception e) {
                        socket3.close();
                    }
                    if (v1 >= v2 && v1 >= v3) {
                        highest = v1;

                    } else if (v2 >= v1 && v2 >= v3) {
                        highest = v2;
                    } else {
                        highest = v3;
                    }
                   // Log.e("client", highest + "");
                    return highest + "";
                }



            } catch (Exception e) {
                e.printStackTrace();

            }
            return String.valueOf(messageCounter.get(node.getMessage()));
        }


        public int writeMessageForReplicas(Node node) {
            int count=0;
            try {

                    if(node.getOperationType().equals("writeVersionsForPropagatedReplication")){
                        Log.e("client","insert "+node.getMessage());
                    Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getNextPort())));
                    Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getNextNextPort())));
                    Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getCurrentPort())));

                    DataOutputStream oos1= new DataOutputStream(socket1.getOutputStream());
                    DataOutputStream oos2= new DataOutputStream(socket2.getOutputStream());
                    DataOutputStream oos3= new DataOutputStream(socket3.getOutputStream());
                    oos1.writeUTF(node.getOperationType()+","+node.getMessage()+","+node.getValue()+","+node.getVersion()+","+node.getCurrentPort());
                    oos2.writeUTF(node.getOperationType()+","+node.getMessage()+","+node.getValue()+","+node.getVersion()+","+node.getCurrentPort());
                    oos3.writeUTF(node.getOperationType()+","+node.getMessage()+","+node.getValue()+","+node.getVersion()+","+node.getCurrentPort());
                    DataInputStream ois1= new DataInputStream(new BufferedInputStream(socket1.getInputStream()));
                    DataInputStream ois2= new DataInputStream(new BufferedInputStream(socket2.getInputStream()));
                    DataInputStream ois3= new DataInputStream(new BufferedInputStream(socket3.getInputStream()));

                    try {
                        String n1= ois1.readUTF();
                        if(n1.equals("replicated")){
                            count=count+1;
                            socket1.close();
                        }

                    } catch (Exception e) {
                        socket1.close();
                    }
                    try {
                        String n2= ois2.readUTF();
                        if(n2.equals("replicated")){
                            count=count+1;
                            socket2.close();
                        }

                    } catch (Exception e) {
                        socket2.close();
                    }
                    try {
                        String n3= ois3.readUTF();
                        if(n3.equals("replicated")){
                            count=count+1;
                            socket3.close();
                        }

                    } catch (Exception e) {
                        socket3.close();
                    }

                }


            } catch (Exception e) {
                e.printStackTrace();

            }

            return count;
        }

        public String[] fromOnePort() {
            try {
                if (node.getOperationType().equals("query")) {
                   Log.e("client","query "+node.getMessage());
                    Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getNextPort())));
                    Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getNextNextPort())));
                    Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getCurrentPort())));

                    DataOutputStream oos1= new DataOutputStream(socket1.getOutputStream());
                    DataOutputStream oos2= new DataOutputStream(socket2.getOutputStream());
                    DataOutputStream oos3= new DataOutputStream(socket3.getOutputStream());
                    oos1.writeUTF(node.getOperationType()+","+node.getMessage());
                    oos2.writeUTF(node.getOperationType()+","+node.getMessage());
                    oos3.writeUTF(node.getOperationType()+","+node.getMessage());
                    DataInputStream ois1= new DataInputStream(new BufferedInputStream(socket1.getInputStream()));
                    DataInputStream ois2= new DataInputStream(new BufferedInputStream(socket2.getInputStream()));
                    DataInputStream ois3= new DataInputStream(new BufferedInputStream(socket3.getInputStream()));
                    int v1 = 0, v2 = 0,v3=0;
                    String value1="",value2="",value3="";
                    try {
                        String n1 = ois1.readUTF();
                        String[] splits=n1.split(",");
                        v1=Integer.parseInt(splits[2]);
                        value1=splits[1];
                        socket1.close();
                    } catch (Exception e) {
                        socket1.close();
                    }
                    try {
                        String n2 = ois2.readUTF();
                        String[] splits=n2.split(",");
                        v2=Integer.parseInt(splits[2]);
                        value2=splits[1];
                        socket2.close();
                    } catch (Exception e) {
                        socket2.close();
                    }
                    try {
                        String n3 = ois3.readUTF();
                        String[] splits=n3.split(",");
                        v3=Integer.parseInt(splits[2]);
                        value3=splits[1];
                        socket3.close();
                    } catch (Exception e) {
                        socket3.close();
                    }
                    if (v1 >= v2 && v1 >= v3) {
                            String[] arr= new String[]{node.getMessage(),value1};
                            return arr;

                    } else if (v2 >= v1 && v2 >= v3) {
                        String[] arr= new String[]{node.getMessage(),value2};
                        return arr;
                    }else{
                        String[] arr= new String[]{node.getMessage(),value3};
                        return arr;
                    }


                }
            } catch (Exception e) {
                e.printStackTrace();
            }	return null;
        }

        public ArrayList<String> fromAllPorts() {
            ArrayList<String> as = new ArrayList<String>();
            try {
                if(node.getOperationType().equals("all")) {
                    Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2* Integer.parseInt(node.getCurrentPort()));
                    DataOutputStream oos1 = new DataOutputStream(socket1.getOutputStream());
                    DataInputStream ois1 = new DataInputStream(new BufferedInputStream(socket1.getInputStream()));
                    oos1.writeUTF("all");
                    String mes= ois1.readUTF();
                    if(!mes.equals("")) {
                        String[] keyValue = mes.split(",");
                        for (String s : keyValue) {
                            String[] star = s.split("&");
                            as.add(star[0] + "," + star[1]);
                        }socket1.close();
                    }
                    socket1.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return as;
        }

        public String deleteQuery(){
            try {
                if (node.getOperationType().equals("delete")) {
                    Log.e("client","delete "+node.getMessage());
                    Log.e("ports",2 * Integer.parseInt(originalHash.get(node.getNextPort()))+"");
                    Log.e("ports",2 * Integer.parseInt(originalHash.get(node.getNextNextPort()))+"");
                    Log.e("ports",2 * Integer.parseInt(originalHash.get(node.getCurrentPort()))+"");
                    Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getNextPort())));
                    Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getNextNextPort())));
                    Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 2 * Integer.parseInt(originalHash.get(node.getCurrentPort())));

                    DataOutputStream oos1= new DataOutputStream(socket1.getOutputStream());
                    DataOutputStream oos2= new DataOutputStream(socket2.getOutputStream());
                    DataOutputStream oos3= new DataOutputStream(socket3.getOutputStream());
                    oos1.writeUTF(node.getOperationType()+","+node.getMessage());
                    oos2.writeUTF(node.getOperationType()+","+node.getMessage());
                    oos3.writeUTF(node.getOperationType()+","+node.getMessage());
                    DataInputStream ois1= new DataInputStream(new BufferedInputStream(socket1.getInputStream()));
                    DataInputStream ois2= new DataInputStream(new BufferedInputStream(socket2.getInputStream()));
                    DataInputStream ois3= new DataInputStream(new BufferedInputStream(socket3.getInputStream()));
                    try {
                        String n1= ois1.readUTF();
                        if(n1.equals("deleted")){
                            socket1.close();
                        }

                    } catch (Exception e) {
                        socket1.close();
                    }
                    try {
                        String n2= ois2.readUTF();
                        if(n2.equals("deleted")){
                            socket2.close();
                        }

                    } catch (Exception e) {
                        socket2.close();
                    }
                    try {
                        String n3= ois3.readUTF();
                        if(n3.equals("deleted")){
                            socket3.close();
                        }

                    } catch (Exception e) {
                        socket3.close();
                    }

                }
            }
            catch(Exception e){
                e.printStackTrace();
            }
            return null;
        }

	}
}
