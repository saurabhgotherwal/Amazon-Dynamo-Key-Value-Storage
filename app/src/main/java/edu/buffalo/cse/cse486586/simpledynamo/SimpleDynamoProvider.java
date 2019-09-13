package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import java.util.Map;


public class SimpleDynamoProvider extends ContentProvider {

    //Separators
    static final String VALUE_SEPARATOR = "~";
    //
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	static final String chordLowestValue = "0000000000000000000000000000000000000000";
	static final String chordHighestValue = "ffffffffffffffffffffffffffffffffffffffff";

	private static String myPort = "";
	private static String myPortHash = "";

	private static String myPredecessor = "";
    private static String myPredecessorHash = "";

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

	private ContentResolver mContentResolver = null;
	private Uri mUri = null;
	private static List<String> NODE_ARRAY = null;
	private static HashMap<String,List<String>> replicatorMap = null;
    private static HashMap<String,List<String>> predecessorMap = null;
    Context appContextGlobal = null;

    private BlockingQueue<String> messageDeliveryAcknowledgementQueue = new LinkedBlockingQueue<String>(1);
    private BlockingQueue<String> messageDeliveryQueue = new LinkedBlockingQueue<String>(1);

    public class VersionClock{
        public String value;
        public int version;
        @Override
        public String toString() {
            return this.value + VALUE_SEPARATOR + this.version;
        }
    }
    public SimpleDynamoProvider() {

    }

	public SimpleDynamoProvider(String port, ContentResolver _cr){

		try {

			myPort = port;
			myPortHash = genHash(port);
			mContentResolver = _cr;
			mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            SetNodeArray();
            SetReplicatorHashMap();
            SetPredecessorHashMap();
            appContextGlobal = getContext();

            boolean amIRecovered = CheckForRecovery();

            if(amIRecovered){
                SyncDataWithOtherNodes();
            }
            else {
                SetFirstTimeCheckPoint();
            }


		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket" + e.getMessage());
			return;
		}
		catch (Exception e) {
			Log.e(TAG, "Can't create a ServerSocket" + e.getMessage());
			return;
		}

	}

    private void SyncDataWithOtherNodes(){
        GetDataFromSuccessor();
        GetDataFromPredecessor();
    }

    private void SetFirstTimeCheckPoint(){
        ContentValues cv = new ContentValues();
        cv.put(KEY_FIELD, "check");
        cv.put(VALUE_FIELD, "I have started for first time") ;
        cv.put("exact", true) ;
        mContentResolver.insert(mUri, cv);
    }

	private boolean CheckForRecovery(){
	    boolean result = false;
        Cursor resultCursor = mContentResolver.query(mUri, null,
                "check", new String[]{"single"}, null);
        if (resultCursor != null && resultCursor.getCount() > 0) {
            result = true;
        }
	    return result;
    }

	private List<String> GetCoordinatorList(String key){
	    List<String> result = new ArrayList<String>();
	    String ownerNode = GetOwnerNode(key);
	    result.add(ownerNode);
	    result.addAll(replicatorMap.get(ownerNode));
	    return result;
    }

	private void SetReplicatorHashMap(){
        replicatorMap = new HashMap<String, List<String>>();
	    List<String> replicator = null;
	    int i = 0;
	    for(String node: NODE_ARRAY){
            replicator = new ArrayList<String>();
	        replicator.add(NODE_ARRAY.get((i+1)%5));
            replicator.add(NODE_ARRAY.get((i+2)%5));
            replicatorMap.put(node,replicator);
            i++;
        }
    }

    private void SetPredecessorHashMap(){
        predecessorMap = new HashMap<String, List<String>>();
        List<String> predecessor = null;
        int i = 0;
        for(String node: NODE_ARRAY){
            try{
                predecessor = new ArrayList<String>();
                int index1 = i-1 < 0 ? (i-1 + 5): i -1;
                int index2 = i-2 < 0 ? (i-2 + 5): i -2;
                predecessor.add(NODE_ARRAY.get(index1));
                predecessor.add(NODE_ARRAY.get(index2));
                predecessorMap.put(node,predecessor);
                if(node.equals(myPort)){
                    myPredecessor = NODE_ARRAY.get(index1);
                    myPredecessorHash = genHash(myPredecessor);
                }
                i++;

            }catch (Exception e){
                Log.e(TAG, "Can't set predecessor hash map" + e.getMessage());
            }

        }
    }

	private void SetNodeArray(){
        NODE_ARRAY = new ArrayList<String>();
        NODE_ARRAY.add("5562");
        NODE_ARRAY.add("5556");
        NODE_ARRAY.add("5554");
        NODE_ARRAY.add("5558");
        NODE_ARRAY.add("5560");
    }

    private String GetOwnerNode(String key){

	    try{
            String predecessorNodeHash = genHash("5560");
            String keyHash = genHash(key);
            for(String node: NODE_ARRAY){
                String currentNodeHash = genHash(node);
                if(IsKeyInRange(currentNodeHash,predecessorNodeHash,keyHash)){
                    return node;
                }
                predecessorNodeHash = currentNodeHash;
            }
        }
        catch (Exception ex){
            Log.e("GetOwnerNode", "Failed to get Owner node");
        }

        return null;
    }

    private boolean IsKeyInRange(String myPortHash, String predecessorNodeHash, String keyHash){

        if (predecessorNodeHash.compareTo(myPortHash) < 0){
            if(predecessorNodeHash.compareTo(keyHash) < 0 && myPortHash.compareTo(keyHash) >= 0){
                return true;
            }
        }
        else {
            if((predecessorNodeHash.compareTo(keyHash) < 0 && chordHighestValue.compareTo(keyHash) >= 0) ||
                    (chordLowestValue.compareTo(keyHash) < 0 && myPortHash.compareTo(keyHash) >= 0)){
                return true;
            }
        }
        return false;
    }

    private void ForwardDeleteRequest(List<String> coordinatorList, String selection){
        String requestType = "DeleteRequest";
        for(String portToForward: coordinatorList){
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,requestType, myPort, portToForward, selection);
        }
    }

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

        if(selection.equals("@")){
            DeleteAllFileFromInternalStorage();
        }
        if(selectionArgs == null){
            List<String> coordinatorList = GetCoordinatorList(selection);
            if(coordinatorList.contains(myPort)){
                DeleteFileFromInternalStorage(selection);
                coordinatorList.remove(myPort);
            }
            ForwardDeleteRequest(coordinatorList, selection);
        }
        else {
            DeleteFileFromInternalStorage(selection);
        }

        return 0;
	}

    private void DeleteFileFromInternalStorage(String filename){

        Context appContext = getContext();
        try {
            appContext.deleteFile(filename);
        } catch (Exception e) {
            Log.e("Delete File", "File delete failed");
        }
    }

    private void DeleteAllFileFromInternalStorage(){

        Context appContext = getContext();
        try {
            String[] listOfFiles = appContext.fileList();
            for(String name: listOfFiles){
                appContext.deleteFile(name);
            }
        } catch (Exception e) {
            Log.e("Delete File", "File delete failed");
        }
    }

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

        Log.e(TAG,"Insert requested : " + values.toString());
        try{
            String key = values.getAsString("key");
            String value = values.getAsString("value");

            if(values.containsKey("rep")){
                String result = WriteMessageIntoInternalStorage(key,value,false,true,true);
                Log.e(TAG,"Insert:  Key: " + key + " insertedValue : " + result);
            }
            else if(values.containsKey("exact")){
                WriteMessageIntoInternalStorage(key,value);
            }

            else if(values.containsKey("insert")){
                String result = WriteMessageIntoInternalStorage(key,value, true, false, true);
                Log.e(TAG,"Insert:  Key: " + key + " insertedValue : " + result);
            }
            else {

                List<String> coordinatorList = GetCoordinatorList(key);
                if(coordinatorList.contains(myPort)){
                    String result = WriteMessageIntoInternalStorage(key,value, true, false,true);
                    coordinatorList.remove(myPort);
                    Log.e(TAG,"Insert:  Key: " + key + " insertedValue : " + result);
                }

                Log.e(TAG,"Insert forwarding request to : " + coordinatorList.toString());
                ForwardInsertRequestToReplicators(key, value, coordinatorList);

            }

        }
        catch (Exception ex){
            Log.e(TAG, "Error in insert " + "  " + ex.toString());
        }
        return uri;
	}
    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        MatrixCursor cursor = null;
        if(selection.equals("@")){
            cursor = ReadAllMessagesFromInternalStorage();
        }
        else if(selection.equals("*")){

            cursor = ReadMessagesFromAllAvds();

        }
        else if(selection.equals("range")){
            cursor = ReadRangeOfMessagesFromInternalStorage(selectionArgs[0], selectionArgs[1]);
        }
        else if(selectionArgs != null && selectionArgs[0].equals("single")){
            cursor = ReadMessageFromInternalStorage(selection, true);
        }
        else if(selectionArgs != null && selectionArgs[0].equals("replica")){
            cursor = ReadMessageFromInternalStorage(selection, false);
        }
        else {
            List<String> coordinatorList = GetCoordinatorList(selection);
            boolean compare = false;
            if(coordinatorList.contains(myPort)){
                cursor = ReadMessageFromInternalStorage(selection, false);
                coordinatorList.remove(myPort);
                compare = true;
            }

            cursor = GetLatestVersionOfSelection(coordinatorList, selection, cursor, compare);

        }
        /*
        References:
        https://stackoverflow.com/questions/18290864/create-a-cursor-from-hardcoded-array-instead-of-db
        */

        String ans = GetMessageFromNormalCursor(cursor);
        Log.e(TAG, "Query: result:  " + ans + " for selection: " + selection);


        return cursor;
    }


    private MatrixCursor ReadRangeOfMessagesFromInternalStorage(String fromNode, String toNode){

        MatrixCursor cursor = new MatrixCursor(
                new String[] {"key", "value"}
        );
        FileInputStream inputStream;
        String value = "";
        Context appContext = getContext();

        try {

            String fromNodeHash = genHash(fromNode);
            String toNodeHash = genHash(toNode);
            String[] listOfFiles = appContext.fileList();
            for(String key: listOfFiles){
                if(key.equals("check")){
                    continue;
                }
                String keyHash = genHash(key);
                boolean result = false;
                if (fromNodeHash.compareTo(toNodeHash) < 0){
                    if(fromNodeHash.compareTo(keyHash) < 0 && toNodeHash.compareTo(keyHash) >= 0){
                        result = true;
                    }
                }
                else {
                    if((fromNodeHash.compareTo(keyHash) < 0 && chordHighestValue.compareTo(keyHash) >= 0) ||
                            (chordLowestValue.compareTo(keyHash) < 0 && toNodeHash.compareTo(keyHash) >= 0)){
                        result = true;
                    }
                }
                if(result){
                    inputStream =  appContext.openFileInput(key);
                    InputStreamReader streamReader = new InputStreamReader(inputStream);
                    BufferedReader bufferedReader = new BufferedReader(streamReader);
                    value = bufferedReader.readLine();
                    cursor.newRow()
                            .add("key", key)
                            .add("value", value);
                    inputStream.close();
                    //Log.e("ReadMessage", "Key: " + key +" value: " + value);
                }

            }

        } catch (Exception e) {
            Log.e("ReadMessage", "File read failed");
        }
        /*
        References:
        https://stackoverflow.com/questions/9095610/android-fileinputstream-read-txt-file-to-string
        */

        return cursor;
    }

    private MatrixCursor ReadMessagesFromAllAvds(){

        MatrixCursor cursor = null;

        try {
            cursor = ReadAllMessagesFromInternalStorage();

            for(String node: NODE_ARRAY){
                if(node.equals(myPort)){
                    continue;
                }
                String messagesFromNode = ForwardGetAllRequestToAllNodes(node);
                if(messagesFromNode != null && !messagesFromNode.toLowerCase().equals("none")){
                    cursor = GetCursorFromMessages(messagesFromNode,cursor);
                }
            }

        } catch (Exception e) {
            Log.e("ReadMessagesFromAllAvds", "File read failed");
        }

        return cursor;
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private synchronized MatrixCursor GetLatestVersionOfSelection(List<String> coordinatorList,String selection,MatrixCursor resultCursor, boolean compare){
        if(resultCursor == null){
            resultCursor = new MatrixCursor(
                    new String[] {"key", "value"}
            );
        }

        MatrixCursor oldCursor = resultCursor;

        for(String portToForward: coordinatorList){
            resultCursor = ForwardGetRequestToOwner(portToForward, selection, resultCursor);
        }


        MatrixCursor newCursor = resultCursor;

        resultCursor = GetLatestVersion(resultCursor);

        if(compare){
            CompareAndInsertLatestVersion(oldCursor,newCursor);
        }

        return resultCursor;
    }

    private void CompareAndInsertLatestVersion(MatrixCursor oldCursor, MatrixCursor newCursor){
        int keyIndex = oldCursor.getColumnIndex(KEY_FIELD);
        int valueIndex = oldCursor.getColumnIndex(VALUE_FIELD);
        String latestValue = "";
        String latestKey = "";

        oldCursor.moveToFirst();
        newCursor.moveToFirst();

        String oldValue = oldCursor.getString(valueIndex);
        int oldVersion = GetVersion(oldValue);

        for (int i = 0; i < newCursor.getCount(); i++) {
            String key = newCursor.getString(keyIndex);
            String value = newCursor.getString(valueIndex);

            int newVersion = GetVersion(value);

            if(newVersion > oldVersion){
                latestKey = key;
                latestValue = value;
                oldVersion = newVersion;
            }
            newCursor.moveToNext();
        }

        if(latestValue != ""){
            WriteMessageIntoInternalStorage(latestKey,latestValue);
        }

    }

    private MatrixCursor GetLatestVersion(MatrixCursor resultCursor){
        MatrixCursor filteredCursor = new MatrixCursor(
                new String[] {"key", "value"}
        );

        int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
        int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
        int maxVersion = -1;
        String latestKey = "";
        String latestValue = "";

        String ans = GetMessageFromNormalCursor(resultCursor);
        Log.e(TAG, "Version content:  " + ans);

        resultCursor.moveToFirst();

        if (resultCursor != null) {

            resultCursor.moveToFirst();
            for (int i = 0; i < resultCursor.getCount(); i++) {
                String key = resultCursor.getString(keyIndex);
                String value = resultCursor.getString(valueIndex);

                Log.e(TAG,"Versioning: Key: " + key + "  Value: " + value);
                int version = GetVersion(value);

                if(version > maxVersion){
                    Log.e(TAG,"Versioning: Key: " + key + "  Value: " + value + "Version: " + version);
                    latestKey = key;
                    latestValue = value.split("~",0)[0];
                    maxVersion = version;
                }
                resultCursor.moveToNext();
            }

            resultCursor.close();
        }

        filteredCursor.newRow()
                .add("key", latestKey)
                .add("value", latestValue);


        return  filteredCursor;

    }

    private int GetVersion(String value){

        int version = -1;
        try {
            if(value != null && value.length() > 0 && value != "" && value != "null" && value.contains("~")){
                version = Integer.parseInt(value.split("~", 0)[1]);
            }
            else{
                version = -1;
            }

        }
        catch (Exception ex){
            Log.e(TAG, "Exception in GetVersion: " + ex.toString() + ". For Value: " + value );
        }
        return version;
    }

    private synchronized MatrixCursor ForwardGetRequestToOwner(String portToForward, String selection, MatrixCursor cursor){

        String result = "none";

        try{
           String requestType = "GetRequest";

           //String result = SendRequest(requestType, myPort, portToForward,selection);

            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,requestType, myPort, portToForward, selection);
            //Log.e(TAG, "Client AA gaya data = " + messageDeliveryQueue.take() );
            result = messageDeliveryQueue.take();

            if(!result.toLowerCase().equals("none"))
                cursor = GetCursorFromMessages(result, cursor);
        }
        catch (Exception ex){

        }

        return cursor;
    }

    private String ForwardGetAllRequestToAllNodes(String portToForward){
        String result = "none";
        try{
            String requestType = "GetAllRequest";

            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,requestType, myPort, portToForward);
            //Log.e(TAG, "Client AA gaya data = " + messageDeliveryQueue.take() );
            result = messageDeliveryQueue.take();
        }
        catch (Exception ex){
        }
        return result;

    }

    private MatrixCursor GetCursorFromMessages(String messages, MatrixCursor cursor){
        String[] list = messages.trim().split("---",0);

        for(String message: list){
            String[] arrOfStr = message.split("#",0);
            String key = arrOfStr[0];
            String value = arrOfStr[1];
            cursor.newRow()
                    .add("key", key)
                    .add("value", value);
        }
        return cursor;
    }

    private MatrixCursor ReadAllMessagesFromInternalStorage(){

        MatrixCursor cursor = new MatrixCursor(
                new String[] {"key", "value"}
        );
        FileInputStream inputStream;
        String value = "";
        Context appContext = getContext();

        try {
            String[] listOfFiles = appContext.fileList();
            for(String key: listOfFiles){
                if(key.equals("check")){
                    continue;
                }
                inputStream =  appContext.openFileInput(key);
                InputStreamReader streamReader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(streamReader);
                value = bufferedReader.readLine();
                String[] arrOfStr = value.split(VALUE_SEPARATOR,0);
                value = arrOfStr[0];
                cursor.newRow()
                        .add("key", key)
                        .add("value", value);
                inputStream.close();

            }

        } catch (Exception e) {
            Log.e("ReadMessage", "File read failed");
        }
        /*
        References:
        https://stackoverflow.com/questions/9095610/android-fileinputstream-read-txt-file-to-string
        */

        return cursor;
    }


    private MatrixCursor ReadMessageFromInternalStorage(String key, boolean filter){

        MatrixCursor cursor = new MatrixCursor(
                new String[] {"key", "value"}
        );

        FileInputStream inputStream;
        String value = "";
        Context appContext = getContext();

        try {
            inputStream =  appContext.openFileInput(key);
            InputStreamReader streamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(streamReader);
            value = bufferedReader.readLine();
            if(filter){
                String[] arrOfStr = value.split(VALUE_SEPARATOR,0);
                value = arrOfStr[0];
            }
            inputStream.close();
            cursor.newRow()
                    .add("key", key)
                    .add("value", value);

            //Log.e("ReadMessage", "Key: " + key +" value: " + value);
        } catch (Exception e) {
            Log.e("ReadMessage", "File read failed");
        }


        /*
        References:
        https://stackoverflow.com/questions/9095610/android-fileinputstream-read-txt-file-to-string
        */

        return cursor;
    }

    private void ForwardInsertRequestToReplicators(String key, String value, List<String> replicators){
        try {

            for(String portToForward: replicators){
                String requestType = "ReplicateRequest";
                String message = key + "#" + value;
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,requestType, myPort, portToForward, message);
            }

        } catch (Exception ex){
            Log.e(TAG, "Error in ForwardInsertRequestToReplicators " + "  " + ex.toString());
        }
    }

    private void ForwardInsertRequestToOwner(ContentValues values, String ownerNodePortNumber){
        try {
            String result = "None";
            String requestType = "InsertRequest";
            String portToForward = ownerNodePortNumber;
            String key = values.getAsString("key");
            String value = values.getAsString("value");
            String message = key + "#" + value;
            String isCoordinatorDown = "false";
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,requestType, myPort, portToForward, message, isCoordinatorDown);
            result = messageDeliveryAcknowledgementQueue.take();
            if(result.contains("false") || result.toLowerCase().contains("none")){
                portToForward = GetNextNode(portToForward);
                if(!portToForward.equals(myPort)){
                    isCoordinatorDown = "true";
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,requestType, myPort, portToForward, message, isCoordinatorDown);
                }
                else {
                    HandleInsertRequestWhenCoordinatorIsDown(key,value);
                }
            }
        } catch (Exception ex){
            Log.e(TAG, "Error in ForwardInsertRequestToOwner " + "  " + ex.toString());
        }
    }

    private String GetNextNode(String port){
        int index = NODE_ARRAY.indexOf(port);
        return NODE_ARRAY.get((index+1)%5);
    }

    private void HandleInsertRequestWhenCoordinatorIsDown(String key, String value){

        ContentValues cv = new ContentValues();
        cv.put("key", key);
        cv.put("value", value);
        cv.put("down", true);
        mContentResolver.insert(mUri, cv);
    }

	private String WriteMessageIntoInternalStorage(String key, String value, boolean incrementVersion, boolean replicateValue, boolean overrideCurrentValue){

	    String content = value;
        if(!replicateValue){
            String previousValue = GetPreviousValue(key);
            if(previousValue == null){
                content = value + VALUE_SEPARATOR + 0 ;
            }
            else {
                content = previousValue;
            }
        }
        else{
            int newVersion = GetVersion(value);

            String previousValue = GetPreviousValue(key);

            if(previousValue != null)
            Log.e(TAG, "previousValue " + "  " + previousValue + " Key:" + key);

            if(previousValue != null){

                int previousVersion = GetVersion(previousValue);
                Log.e(TAG, "previousVersion " + "  " + previousVersion + " newVersion: " + newVersion +" Key:" + key);
                if(newVersion < previousVersion){
                    return previousValue;
                }
            }
        }


        VersionClock clock = GetVersionClock(key,content,incrementVersion);
        if(overrideCurrentValue){
            clock.value = value.split("~",0)[0];
        }
        content = clock.toString();

        WriteMessageIntoInternalStorage(key,content);

        return content;
    }

    private VersionClock GetVersionClock(String key, String value, boolean incrementVersion){
        VersionClock result = new VersionClock();
        String[] arrOfStr = value.split(VALUE_SEPARATOR,0);
        result.value = arrOfStr[0];

        if(incrementVersion){
            result.version = Integer.parseInt(arrOfStr[1]) + 1;
        }
        else {
            result.version = Integer.parseInt(arrOfStr[1]);
        }

        return result;
    }


    private String GetPreviousValue(String key){

        Cursor resultCursor = ReadMessageFromInternalStorage(key, false);

        if (resultCursor == null || resultCursor.getCount() < 1) {
            return null;
        }

        resultCursor.moveToFirst();
        int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);

        String returnValue = resultCursor.getString(valueIndex);

        return returnValue;
    }

    private void WriteMessageIntoInternalStorage(String filename, String content){

        FileOutputStream outputStream;
        Context appContext = getContext();

        try {
            outputStream =  appContext.openFileOutput(filename, Context.MODE_PRIVATE);
            outputStream.write(content.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e("WriteMessage", "File write failed");
        }

        /*
        References:
        Usage has been referred from PA1.
        */
    }

    private String GetMessageFromNormalCursor(Cursor cursor){
        StringBuilder result = new StringBuilder();

        int keyIndex = cursor.getColumnIndex(KEY_FIELD);
        int valueIndex = cursor.getColumnIndex(VALUE_FIELD);

        if (cursor != null) {

            cursor.moveToFirst();
            for (int i = 0; i < cursor.getCount(); i++) {
                if(i != 0){
                    result.append("---");
                }
                String key = cursor.getString(keyIndex);
                String value = cursor.getString(valueIndex);

                if(value != null && value.length() > 0 && value != "null")
                    result.append(key + "#" + value);

                cursor.moveToNext();
            }
            cursor.close();
        }

        return result.toString();

        /*
        https://stackoverflow.com/questions/10081631/android-cursor-movetonext
        */
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

    private void GetDataFromSuccessor(){
        List<String> replicator = replicatorMap.get(myPort);
        String requestType = "GetMyDataRequest";
        for(String portToRequest: replicator) {
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, requestType, myPort, portToRequest,myPredecessor,myPort);
        }
    }

    private void GetDataFromPredecessor(){
        List<String> predecessor = predecessorMap.get(myPort);
        String requestType = "GetYourDataRequest";
        for(String portToRequest: predecessor){
            try{
                Thread.sleep(50);
            }catch (Exception ex){

            }
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,requestType, myPort, portToRequest);
        }
    }

    private String GetFormattedMessage(String requestType, String myPort, String... arguments){
        String formattedMessage = "";

        if(requestType.equals("InsertRequest")){
            formattedMessage="RequestType=InsertRequest&"+"MyPort=" + myPort + "&Message=" + arguments[3] + "&IsCoordinatorDown=" + arguments[4];
        }
        else if(requestType.equals("ReplicateRequest")){
            formattedMessage="RequestType=ReplicateRequest&"+"MyPort=" + myPort + "&Message=" + arguments[3];
        }
        else if(requestType.equals("GetAllRequest")){
            formattedMessage="RequestType=GetAllRequest&"+"MyPort=" + myPort;
        }
        else if(requestType.equals("GetRequest")){
            formattedMessage="RequestType=GetRequest&"+"MyPort=" + myPort + "&Selection=" + arguments[3];
        }
        else if(requestType.equals("DeleteRequest")){
            formattedMessage="RequestType=DeleteRequest&"+"MyPort=" + myPort + "&Selection=" + arguments[3];
        }
        else if(requestType.equals("GetMyDataRequest")){
            formattedMessage="RequestType=GetMyDataRequest&"+"MyPort=" + myPort + "&FromNodePortNumber=" + arguments[3] + "&ToNodePort=" + arguments[4] ;
        }
        else if(requestType.equals("GetYourDataRequest")){
            formattedMessage="RequestType=GetYourDataRequest&"+"MyPort=" + myPort;
        }
        if(requestType.equals("MoveMessages")){
            formattedMessage="RequestType=MoveMessages&"+"MyPort=" + myPort + "&Message=" + arguments[3];
        }

        return formattedMessage;
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        protected Void doInBackground(String... arguments) {

            String requestType = arguments[0];
            String myPort = arguments[1];
            String portToConnect = String.valueOf((Integer.parseInt(arguments[2]) * 2));
            int timeout = 2000;
            String insertRequestResult = "None";
            String getAllRequestResult = "none";

            String msgToSend =  GetFormattedMessage(requestType, myPort,arguments);
            Log.e(TAG, "ClientTask = Message:" + msgToSend + " Port:" + portToConnect);
            if(msgToSend == ""){
                return null;
            }

            Socket socket = new Socket();
            try{

                SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portToConnect));

                socket.connect(socketAddress, timeout);

                PrintWriter outputPrintWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader inputBufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                outputPrintWriter.println(msgToSend);
                String messageFromServer = "";
                while ((messageFromServer = inputBufferedReader.readLine()) != null) {

                    Log.e(TAG, "ClientTask: Message from server:" + messageFromServer + " Port:" + portToConnect);

                    if(messageFromServer.contains("InsertRequest")){
                        insertRequestResult = "true";
                    }
                    if(messageFromServer.contains("GetRequestAllResult")){
                        getAllRequestResult = messageFromServer.substring(20);
                    }
                    if(messageFromServer.contains("GetRequestResult")){
                        getAllRequestResult = messageFromServer.substring(17);
                    }
                    if (messageFromServer.contains("MESSAGE-RECEIVED")) {
                        break;
                    }
                }

                inputBufferedReader.close();
                outputPrintWriter.close();
                socket.close();


            }catch (SocketTimeoutException e) {
                Log.e(TAG, "ClientTask Socket timeout: " + portToConnect);
                insertRequestResult = "false";
            }catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException: " + portToConnect);
                insertRequestResult = "false";
            }catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException 1: " + e.getMessage() + "  " + e.getStackTrace() + " Port:  " + portToConnect);
                insertRequestResult = "false";
            }catch (Exception ex){
                Log.e(TAG, "ClientTask Exception: " + ex.getMessage() + ": Port: " + portToConnect);
                insertRequestResult = "false";
            }finally {
                try {
                    socket.close();
                    if (requestType.equals("InsertRequest")){
                        messageDeliveryAcknowledgementQueue.put(insertRequestResult);
                    }
                    if (requestType.contains("GetAllRequest") || requestType.contains("GetRequest")){
                        messageDeliveryQueue.put(getAllRequestResult);
                    }
                }catch (Exception ex){
                    Log.e(TAG, "ClientTask Exception: " + ex.getMessage() + ": Port: " + portToConnect);
                }
            }

            return null;
        }
    }


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		protected Void doInBackground(ServerSocket... sockets) {


			ServerSocket serverSocket = sockets[0];

			try{
				while(true){
					Socket socket = serverSocket.accept();
					socket.setSoTimeout(500);
					try{

						PrintWriter outputPrintWriter = new PrintWriter(socket.getOutputStream(), true);
						BufferedReader inputBufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String messageFromClient = "";

						while ((messageFromClient = inputBufferedReader.readLine()) != null) {

                            Log.e(TAG, "ServerTask: Message received from client:" + messageFromClient);

							if(messageFromClient.contains("InsertRequest")){
                                publishProgress(messageFromClient);
								String reply = "InsertRequestResult=true";
								outputPrintWriter.println(reply);
								outputPrintWriter.println("MESSAGE-RECEIVED");
								break;
							}
                            else if(messageFromClient.contains("GetAllRequest")){
                                String reply = "GetRequestAllResult=" + StartProcessToGetAll();
                                outputPrintWriter.println(reply);
                                outputPrintWriter.println("MESSAGE-RECEIVED");
                                break;
                            }
                            else if(messageFromClient.contains("GetRequest")){
                                String strReceived = messageFromClient.trim();
                                String[] arrOfStr = strReceived.split("&",0);
                                String selection = arrOfStr[2].substring(10);
                                String reply = "GetRequestResult=" + StartProcessToGet(selection);
                                outputPrintWriter.println(reply);
                                outputPrintWriter.println("MESSAGE-RECEIVED");
                                break;
                            } else if(messageFromClient.contains("ReplicateRequest")){
                                String strReceived = messageFromClient.trim();
                                String[] arrOfStr = strReceived.split("&",0);
                                String message = arrOfStr[2].substring(8);
                                StartProcessToReplicate(message);
                                outputPrintWriter.println("MESSAGE-RECEIVED");
                                break;
                            }
                            else if(messageFromClient.contains("GetMyDataRequest")){
                                String strReceived = messageFromClient.trim();
                                String[] arrOfStr = strReceived.split("&",0);
                                String portToNotify = arrOfStr[1].substring(7);
                                String fromNode = arrOfStr[2].substring(19);
                                String toNode = arrOfStr[3].substring(11);
                                MoveMessagesToRequestor(portToNotify,fromNode,toNode);
                                outputPrintWriter.println("MESSAGE-RECEIVED");
                                break;
                            }
                            else if(messageFromClient.contains("GetYourDataRequest")){
                                String strReceived = messageFromClient.trim();
                                String[] arrOfStr = strReceived.split("&",0);
                                String portToNotify = arrOfStr[1].substring(7);
                                String fromNode = myPredecessor;
                                String toNode = myPort;
                                MoveMessagesToRequestor(portToNotify,fromNode,toNode);
                                outputPrintWriter.println("MESSAGE-RECEIVED");
                                break;
                            }
                            else if(messageFromClient.contains("MoveMessages")){
                                String strReceived = messageFromClient.trim();
                                String[] arrOfStr = strReceived.split("&",0);
                                String message = arrOfStr[2].substring(8);
                                InsertMessagesToAvd(message);
                                outputPrintWriter.println("MESSAGE-RECEIVED");
                                break;
                            }
							else {
                                publishProgress(messageFromClient);
                                outputPrintWriter.println("MESSAGE-RECEIVED");
                                break;
                            }

						}
						inputBufferedReader.close();
						outputPrintWriter.close();
						socket.close();

					}catch (SocketTimeoutException e) {
						Log.e(TAG, "ServerTask Socket timeout: ");
					}catch (IOException e) {
						Log.e(TAG, "ServerTask IOException : " + e.getMessage() + "  " + e.getStackTrace());
					}catch (Exception ex){
						Log.e(TAG, "ServerTask Exception: " + ex.getMessage());
					}finally {
						socket.close();
					}

				}

			} catch (Exception ex){
				Log.e(TAG, "ServerTask Exception: " + ex.getMessage());
			}
			finally{
				try{
					serverSocket.close();
				} catch (Exception ex){
					Log.e(TAG, "ServerTask Exception: " + ex.getMessage());
				}
			}

			return null;
		}

		protected synchronized void onProgressUpdate(String...strings) {

			String strReceived = strings[0].trim();
			String[] arrOfStr = strReceived.split("&",0);

            if(strReceived.contains("InsertRequest")){
                String message = arrOfStr[2].substring(8);
                String isCoordinatorDown = arrOfStr[3].substring(18);
                StartProcessToInsert(message, isCoordinatorDown);
            }
            else if(strReceived.contains("DeleteRequest")){
                String selection = arrOfStr[2].substring(10);
                StartProcessToDelete(selection);
            }

			return;
		}

        private synchronized void InsertMessagesToAvd(String messages){

            String[] list = messages.split("---",0);

            for(String message: list){
                String[] arrOfStr = message.split("#",0);
                if(arrOfStr[0] == "")
                    break;
                String key = arrOfStr[0];
                String value = arrOfStr[1];
                ContentValues cv = new ContentValues();
                cv.put("key", key);
                cv.put("value", value);
                cv.put("rep", true);
                try {
                    mContentResolver.insert(mUri, cv);

                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }
        }

        private void StartProcessToReplicate(String message){
            try{
                String[] arrOfStr = message.split("#",0);
                String key = arrOfStr[0];
                String value = arrOfStr[1];
                try {

                    ContentValues cv = new ContentValues();
                    cv.put("key", key);
                    cv.put("value", value);
                    cv.put("insert", true);
                    mContentResolver.insert(mUri, cv);
                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }
            catch (Exception ex){

            }
        }

        private void StartProcessToDelete(String selection){

            try {
                mContentResolver.delete(mUri,selection,new String[]{"single"});

            } catch (Exception e) {
                Log.e(TAG, e.toString());
            }

        }

        private String StartProcessToGet(String selection){
            try{

                try {
                    Cursor result = mContentResolver.query(mUri, null,selection,new String[]{"replica"},null);
                    return (GetMessageFromNormalCursor(result));

                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }
            catch (Exception ex){

            }
            return null;
        }

        private String StartProcessToGetAll(){
            try{

                try {
                    Cursor result = mContentResolver.query(mUri, null,"@",null,null);
                    return (GetMessageFromNormalCursor(result));

                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }
            catch (Exception ex){

            }
            return null;
        }



        private void StartProcessToInsert(String message, String isCoordinatorDown){
            try{
                String[] arrOfStr = message.split("#",0);
                String key = arrOfStr[0];
                String value = arrOfStr[1];
                try {
                    if(isCoordinatorDown.contains("false")){
                        ContentValues cv = new ContentValues();
                        cv.put("key", key);
                        cv.put("value", value);
                        mContentResolver.insert(mUri, cv);
                    }
                    else {
                        HandleInsertRequestWhenCoordinatorIsDown(key,value);
                    }
                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }
            catch (Exception ex){

            }
        }

        private void MoveMessagesToRequestor(String portToNotify, String fromNode, String toNode){
            String requestType = "MoveMessages";
            String messagesToMove = GetMessagesToMove(fromNode,toNode);
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,requestType, myPort, portToNotify, messagesToMove);
        }

        private String GetMessagesToMove(String fromNode, String toNode){
            String result = "";

            try {
                Cursor cursor = mContentResolver.query(mUri, null, "range", new String[]{fromNode, toNode}, null);

                result = GetFormatedMessageFromCursor(cursor);
            }
            catch (Exception ex){
                Log.e("GetMessagesToMove", "Message: " + ex.getMessage());
            }

            return result;
        }

        private String GetFormatedMessageFromCursor(Cursor cursor){
            StringBuilder result = new StringBuilder();

            int keyIndex = cursor.getColumnIndex(KEY_FIELD);
            int valueIndex = cursor.getColumnIndex(VALUE_FIELD);

            if (cursor != null) {

                cursor.moveToFirst();
                for (int i = 0; i < cursor.getCount(); i++) {
                    if(i != 0){
                        result.append("---");
                    }
                    String key = cursor.getString(keyIndex);
                    String value = cursor.getString(valueIndex);

                    result.append(key + "#" + value);

                    cursor.moveToNext();
                }
                cursor.close();
            }

            return result.toString();

        /*
        https://stackoverflow.com/questions/10081631/android-cursor-movetonext
        */
        }

	}
}
