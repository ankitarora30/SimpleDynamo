package edu.buffalo.cse.cse486586.simpledynamo;

import java.net.SocketTimeoutException;

import android.telephony.TelephonyManager;

import java.util.Map;

import android.database.MatrixCursor;
import android.database.sqlite.SQLiteQueryBuilder;

import java.io.DataInputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;

import android.os.AsyncTask;
import android.os.ConditionVariable;
import android.util.Log;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;

import android.content.Context;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteDatabase;

import edu.buffalo.cse.cse486586.simpledynamo.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;

public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    static String[] hash_array=new String[5];
    static String[] port_array=new String[5];
    static HashMap<String,String> query_star=new HashMap<String,String>(); 
    static boolean flag_created_once=false;
    String myPort="null";
    static boolean flag_server_task=true;
    static ConditionVariable cv=new ConditionVariable(true);


    /*Mapping between the nodes in the distributed systems and their port numbers */
    static void initialize_mapping(){
        try {
            hash_array[0]=genHash("5562");
            hash_array[1]=genHash("5556");
            hash_array[2]=genHash("5554");
            hash_array[3]=genHash("5558");
            hash_array[4]=genHash("5560");

            port_array[0]=REMOTE_PORT4;
            port_array[1]=REMOTE_PORT1;
            port_array[2]=REMOTE_PORT0;
            port_array[3]=REMOTE_PORT2;
            port_array[4]=REMOTE_PORT3;

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    DBHelper db;
    private SQLiteDatabase OurDatabase;

    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";

    private static final String DATABASE_NAME="SimpleDht.db"; 
    public static final String DATABASE_TABLE = "Messages";



    /*The data base sub-class with create and update method*/
    private static class DBHelper extends SQLiteOpenHelper{

        /*constructor*/
        public DBHelper(Context context) {
            super(context, DATABASE_NAME, null,1);
        }

        public void onCreate(SQLiteDatabase db) {
            db.execSQL("CREATE TABLE " + DATABASE_TABLE + " (" +
                    KEY_FIELD + " TEXT NOT NULL, " + 
                    VALUE_FIELD + " TEXT NOT NULL);");
            flag_created_once=true;
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        }

    }	



    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Socket request_socket;

        /*Deletes data from all the 5 nodes in the system using the given selection*/
        for(int j=0;j<5;j++){
            try{
                request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port_array[j]));
                String msg ="delete"+"|"+ selection+"\n";
                PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
                seq_out.append(msg);
                seq_out.flush();
                seq_out.close();
                request_socket.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        return 0;
    }



    @Override
    public String getType(Uri uri) {
        return null;
    }


    /*inserts data in the correct node and two nodes further*/
    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String key=values.getAsString("key");
        String value=values.getAsString("value");
        String key_hash="";
        String time=String.valueOf(System.nanoTime());
        String msg ="data_insert"+"|"+key+"|"+value+"|"+time;
        try {
            key_hash=genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        /*corner case in the ring formation*/
        if((key_hash.compareTo(hash_array[0])<0)||key_hash.compareTo(hash_array[4])>0)
        {
            Socket request_socket;
            try {
                request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(REMOTE_PORT4));
                PrintStream seq_out1 = new PrintStream(request_socket.getOutputStream());            
                seq_out1.append(msg);
                seq_out1.flush();
                request_socket.close();
                request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(REMOTE_PORT1));
                PrintStream seq_out2 = new PrintStream(request_socket.getOutputStream());            
                seq_out2.append(msg);
                seq_out2.flush();
                request_socket.close();
                request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(REMOTE_PORT0));
                PrintStream seq_out3 = new PrintStream(request_socket.getOutputStream());            
                seq_out3.append(msg);
                seq_out3.flush();
                request_socket.close();
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }      
        else
        {
            for(int i =0;i<4;i++)
            {
                if(key_hash.compareTo(hash_array[i])>0 && key_hash.compareTo(hash_array[i+1])<0)
                {
                    for(int j=i+1;j<i+4;j++)
                    {

                        Socket request_socket;
                        try {
                            request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(port_array[j]));
                            PrintStream seq_out1 = new PrintStream(request_socket.getOutputStream());            
                            seq_out1.append(msg);
                            seq_out1.flush();
                            request_socket.close();
                        } catch (NumberFormatException e) {
                            e.printStackTrace();
                        } catch (UnknownHostException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                }
            }
        }
        return null;
    }


    @Override
    public boolean onCreate() 
    {
        Context context = getContext();
        db=new DBHelper(context);
        OurDatabase=db.getWritableDatabase();	

        /*calculate the port number,logic for port forwarding*/
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        initialize_mapping();

        /*check if the node is recovering and if true : recover*/
        if(!flag_created_once)
        {		    
            flag_server_task=false;
            recover();			
        }

        try 
        {		
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e){	
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
        return false;
    }


    /*if the node is recover mode, create new thread to process recovery*/
    public void recover()
    {
        String flag="recover";
        try{
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, flag , myPort);
        }
        catch (Exception e)
        {
            Log.v(TAG, "ClientTask Exception");
        }
    }


    /*find the required data*/
    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
            String[] selectionArgs, String sortOrder) {
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(DATABASE_TABLE);
        Cursor cursor=null;
        String serialize="";
        DataInputStream query_input;
        String query_reply;
        DataInputStream input;
        Socket request_socket;

        /*to query all the cumulative data in the system*/
        if(selection.equals("*"))
        {
            /*send query request to all the nodes*/
            for(int j=0;j<5;j++)
            {
                try
                {
                    request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port_array[j]));
                    String msg ="query"+"|"+ selection+"\n";

                    PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
                    seq_out.append(msg);
                    seq_out.flush();

                    input= new DataInputStream(request_socket.getInputStream());
                    request_socket.setSoTimeout(1000);

                    query_reply=input.readLine();
                    if(query_reply==null)
                        continue;

                    serialize=query_reply;
                    seq_out.close();
                    input.close();
                    request_socket.close();

                    /*de-serialize*/
                    String tuple[]=serialize.split("#");
                    for(String s:tuple)
                    {
                        String array[]=s.split("\\|");
                        String key = array[0].trim();
                        String value = array[1].trim();
                        query_star.put(key, value);
                    }
                }
                catch (IOException e) {
                    Log.v(TAG, "Exception in query");
                    e.printStackTrace();
                }
            }	

            /*create matrix cursor to return */
            String col[]={"key","value"};
            MatrixCursor MC=new MatrixCursor(col);

            for (Map.Entry<String, String> entry : query_star.entrySet()) 
            {
                String key = entry.getKey();
                String value = entry.getValue();
                MC.addRow(new String[] {key,value});
            }
            return MC;
        }

        /*query local DB of the node*/
        else if(selection.equals("@"))
        {
            cv.block();
            cursor=OurDatabase.rawQuery("SELECT * FROM Messages", null);	
            if(cursor.getCount()!=0)
            {	
                cursor.moveToFirst();

                /*serialize the query result from the cursor*/
                while(cursor.isAfterLast()==false)
                {
                    String key=cursor.getString(0);
                    serialize+=key;
                    serialize=serialize+"|";
                    String value=cursor.getString(1);
                    serialize=serialize+value;
                    serialize=serialize+"#";
                    cursor.moveToNext();
                }

                String tuple[]=serialize.split("#");
                String col[]={"key","value"};
                MatrixCursor MC=new MatrixCursor(col);

                /*create matrix cursor to return */
                for(String s:tuple)
                {
                    String array[]=s.split("\\|");
                    String key = array[0].trim();
                    String value = array[1].trim();
                    MC.addRow(new String[] {key,value});
                }
                return MC;
            }
        }
        else /*single key query in the correct node and the replicas*/
        {
            try {
                String key_hash=genHash(selection);
                if(key_hash.compareTo(hash_array[0])<0 || key_hash.compareTo(hash_array[4])>0)
                {
                    for(int j=0;j<3;j++)
                    {
                        request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port_array[j]));
                        String msg ="query"+"|"+ selection+"\n";

                        PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
                        seq_out.append(msg);
                        seq_out.flush();
                        try
                        {
                            input= new DataInputStream(request_socket.getInputStream());
                            request_socket.setSoTimeout(1000);

                            query_reply=input.readLine();
                            if(query_reply!=null)
                            {
                                serialize=query_reply;
                            }
                            seq_out.close();
                            input.close();
                            request_socket.close();
                        }
                        catch (IOException e) {
                            Log.v("exception", "timeout exception");
                            e.printStackTrace();
                        }
                    }
                }
                else
                {
                    for(int i =0;i<4;i++)
                    {
                        if(key_hash.compareTo(hash_array[i])>0 && key_hash.compareTo(hash_array[i+1])<0)
                        {
                            for(int j=i+1;j<i+4;j++)
                            {
                                request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(port_array[j%5]));
                                String msg ="query"+"|"+ selection+"\n";

                                PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
                                seq_out.append(msg);
                                seq_out.flush();
                                try{
                                    input= new DataInputStream(request_socket.getInputStream());
                                    request_socket.setSoTimeout(1000);

                                    query_reply=input.readLine();
                                    if(query_reply!=null)
                                    {
                                        serialize=query_reply;
                                    }
                                    seq_out.close();
                                    input.close();
                                    request_socket.close();
                                }
                                catch (IOException e) {
                                    Log.v("exception", "timeout exception");
                                    e.printStackTrace();
                                }
                            }
                            break;
                        }
                    }
                }
                String tuple[]=serialize.split("#");
                String col[]={"key","value"};
                MatrixCursor MC=new MatrixCursor(col);
                for(String s:tuple)
                {
                    String array[]=s.split("\\|");
                    String key = array[0].trim();
                    String value = array[1].trim();
                    MC.addRow(new String[] {key,value});
                }
                return MC;
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return cursor;		
    }

    
    
    private static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    
    
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @SuppressWarnings("deprecation")
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            Socket soc=null;
            DataInputStream input;
            String s;
            while(true)
            {
                try{
                    soc=serverSocket.accept();
                    input= new DataInputStream(soc.getInputStream());
                    s=input.readLine();
                    String seq_request[] = s.split("\\|");

                    /*handle query request*/
                    if(s.contains("query"))
                    {
                        cv.block();
                        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();						
                        Cursor cursor=null;
                        String serialize="";
                        String selection=seq_request[1];
                        String where="";
                        if(!(selection.equals("*") ))
                        {
                            where="WHERE key = '" + selection +"'";
                        }
                        cursor=OurDatabase.rawQuery("SELECT * FROM Messages "+where , null);	
                        cursor.moveToFirst();

                        /*serialize the query result to send it across network*/
                        while(cursor.isAfterLast()==false)
                        {
                            String key=cursor.getString(0);
                            serialize+=key;
                            serialize=serialize+"|";
                            String value=cursor.getString(1);
                            serialize=serialize+value;
                            serialize=serialize+"#";
                            cursor.moveToNext();
                        }

                        serialize=serialize+"\n";
                        PrintStream reply_soc=new PrintStream(soc.getOutputStream());
                        reply_soc.append(serialize);
                        reply_soc.flush();
                    }

                    /*handle insert request*/
                    else if(s.contains("data_insert"))
                    {
                        String key=seq_request[1];
                        String value=seq_request[2]+"|"+seq_request[3];
                        ContentValues values = new ContentValues();
                        values.put("key",key);
                        values.put("value",value);
                        OurDatabase.insert(DATABASE_TABLE, null, values);
                    }
                    
                    /*delete the selection as per the request*/
                    else if(s.contains("delete"))
                    {
                        String selection=seq_request[1];
                        OurDatabase.delete(DATABASE_TABLE, //table name
                                "key='"+selection + "'",  // selections
                                null); //selections args  
                    }
                }
                catch(IOException e){
                    Log.e(TAG, "Error");
                }         
            }
        }

        
        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        protected void onProgressUpdate(String...strings) {
            return;
        }
    }

    
    
    private class ClientTask extends AsyncTask<String, Void, Void> 
    {

        /*recovery of the node*/
        @Override
        protected Void doInBackground(String... msgs) {
            try{
                if(msgs[0].contains("recover"))
                {
                    cv.close();
                    DataInputStream query_input;
                    String query_reply;
                    DataInputStream input;
                    Socket request_socket;
                    String serialize="";

                    String gen_myPort=genHash(myPort);

                    HashMap<String,String> recover_data= new HashMap<String,String>();

                    /*query the replicas to fetch data*/
                    for(int i=0;i<5;i++)
                    {
                        try
                        {
                            if(port_array[i].equals(myPort))
                                continue;
                            else
                            {
                                request_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(port_array[i]));
                                String msg ="query" + "|" + "*" + "\n";
                                PrintStream seq_out = new PrintStream(request_socket.getOutputStream());            
                                seq_out.append(msg);
                                seq_out.flush();

                                input= new DataInputStream(request_socket.getInputStream());
                                request_socket.setSoTimeout(1200);
                                query_reply=input.readLine();
                                if(query_reply==null)
                                {
                                    query_reply="";	
                                    continue;
                                }
                                serialize=query_reply;
                                seq_out.close();
                                input.close();
                                request_socket.close();
                                if(serialize=="")
                                {
                                    continue;
                                }
                                String tuple[]=serialize.split("#");
                                for(String s:tuple)
                                {
                                    String array[]=s.split("\\|");
                                    String new_key = array[0].trim();
                                    String gen_new_key=genHash(new_key);

                                    if( 
                                            ( (myPort.equals(REMOTE_PORT4)) && (gen_new_key.compareTo(hash_array[2])<0) && (gen_new_key.compareTo(hash_array[0])>0) ) 
                                            ||(	(myPort.equals(REMOTE_PORT1)) && (gen_new_key.compareTo(hash_array[3])<0) && (gen_new_key.compareTo(hash_array[1])>0) )	 		
                                            ||(	(myPort.equals(REMOTE_PORT0)) && (gen_new_key.compareTo(hash_array[4])<0) && (gen_new_key.compareTo(hash_array[2])>0) )		
                                            ||(	(myPort.equals(REMOTE_PORT2)) && ((gen_new_key.compareTo(hash_array[0])<0) || (gen_new_key.compareTo(hash_array[3])>0)) )
                                            ||(	(myPort.equals(REMOTE_PORT3)) && ((gen_new_key.compareTo(hash_array[1])<0) || (gen_new_key.compareTo(hash_array[4])>0)) )		
                                            )
                                    {
                                        continue;
                                    }

                                    String new_value = array[1].trim();
                                    String new_time = array[2].trim();

                                    /*deserialize the recovered data and insert into DB*/
                                    if(recover_data.containsKey(new_key))
                                    {
                                        String existing_tuple="";
                                        existing_tuple=recover_data.get(new_key);
                                        String existing_array[]=existing_tuple.split("\\|");
                                        String existing_time=existing_array[1];
                                        if(new_time.compareTo(existing_time)>0)
                                        {
                                            recover_data.put(new_key,new_value+"|"+new_time);										
                                        }
                                    }
                                    else
                                    {
                                        recover_data.put(new_key,new_value+"|"+new_time);
                                    }
                                }
                            }
                        }
                        catch (SocketTimeoutException e){
                            Log.v(TAG, "Socket timeout Exception for"+port_array[i]);							
                            e.printStackTrace();
                        }
                        catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                        catch (SocketException e){
                            e.printStackTrace();							
                        }
                    }

                    String selection="*";
                    OurDatabase.delete(DATABASE_TABLE, //table name
                            "1",  // selections
                            null); //selections args  

                  /*insert the recovered data in the DB*/  
                    for (Map.Entry<String,String> entry : recover_data.entrySet())
                    {
                        String key=entry.getKey();
                        String value=entry.getValue();
                        ContentValues values = new ContentValues();
                        values.put("key",key);
                        values.put("value",value);
                        OurDatabase.insert(DATABASE_TABLE, null, values);
                    }
                    cv.open();
                    flag_server_task=true;
                }
                else
                {
                    return null;
                }
            } catch (UnknownHostException e) {
            } catch (IOException e) {
            } catch (NoSuchAlgorithmException e1) {
                e1.printStackTrace();
            }
            return null;
        }
    }

    
    
    @Override
    public int update(Uri arg0, ContentValues arg1, String arg2, String[] arg3) {
        return 0;
    }
}