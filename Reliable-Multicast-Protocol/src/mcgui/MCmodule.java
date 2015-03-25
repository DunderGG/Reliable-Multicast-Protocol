package mcgui;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

/*
 * 		AUTHOR: 	David Bennehag (David.Bennehag@Gmail.com)
 * 		VERSION: 	1.0
 * 
 * 		COURSE: Distributed Systems, Advanced (MPCSN, Chalmers University of Technology)
 *
 *		SOURCE: 	E:\Program\Git\repository\Reliable-Multicast-Protocol\Reliable-Multicast-Protocol\src
 *					~/git/Reliable-Multicast-Protocol/Reliable-Multicast-Protocol/src
 *
 *		RUN WITH: 	java mcgui.Main mcgui.MCmodule 1 mcgui/localhostsetup		
 *
 *		TODO  
 *				
 *
 * 
 * 		FINISHED
 * 			Implement a reliable (and totally ordered) multicast, visualized by a provided GUI and pre-setup TCP connections.
 *				Needs to cope with crashing processes, but not re-joining processes.
 * 
 * 			Lowest ID (ID 0, first in the file) is the sequencer in the start. Next lowest becomes Shadow Sequencer.
 * 				New sequencers are then chosen in ascending order.
 * 			Shadow Sequencer will, when primary sequencer crashes, take over the job and replay messages
 * 
 */

public class MCmodule extends Multicaster implements MulticasterUI
{
	//Only used for debugging purposes
	private long startTime;
	private boolean debug, extensiveDebug;
	
	//TIMER NOT YET USED
	//private repeatingTimer timer;
	
	//Different types of messages
	private static final int NORMAL = 0;
	private static final int ACK = 1;
	private static final int COMMIT = 2;
	private static final int RESEND = 3;
	private static final int SHWNRM = 4;
	
	//Will keep track of the current sequencer
	private int sequencer;
	private int shadowSeq;
	
	//Will keep track of which message was last delivered, so we don't deliver a message too early or more than once.
	private int lastDelivered;
	
	//Pretty self-descriptive....
	private int numberOfClients, myPort;
	
	//The logical clock
	private int logClock;
	//The vector clock will be used by the sequencer to keep track of each hosts current logical clock
	private int[] vectorClock = {0,0,0};

	//Will be a list containing messages that have been received but cannot be delivered yet
	private ArrayList<MCmessage> msgList;
	//Will be a list containing messages, saved by the hosts, that might need to be retransmitted.
	private ArrayList<MCmessage> savedMsgList;
	//Used by the shadow sequencer to replay messages
	private ArrayList<MCmessage> shadowList;
	
	//Will contain the contents of the setup file, could probably skip this step...
	private ArrayList<String[]> setup;
	
	//Will contain ID, host name and port from the setup file
	private ArrayList<String[]> hostList;
	
	//Whenever we send a NORMAL message, we need to collect ACKs from the other nodes.
	//	The ACKs will be stored in ackList. 
	//	The key will be the messageID and the value is number of ACKs received
	private static HashMap<String, Integer> ackList = new HashMap<String, Integer>();

	public static HashMap<String, Integer> getAckList()
	{
		return ackList;
	}

	/*
	 * Constructor
	 */
	public MCmodule()
	{		
		//For debugging purposes
		startTime = System.currentTimeMillis();
		debug = true;
		extensiveDebug = false;
		
		//Initialize our variables
		numberOfClients = 0;
		myPort = 0;
	
		logClock = 0;
		
		sequencer = 0;
		shadowSeq = sequencer + 1;
		
		lastDelivered = -1;
				
		msgList = new ArrayList<MCmessage>();
		savedMsgList = new ArrayList<MCmessage>();
		shadowList = new ArrayList<MCmessage>();
		
		setup = new ArrayList<String[]>();
		
	}
	
	/*
	 * Extended from the abstract class Multicaster.
	 * Used to initialize stuff and do some basic printing for debugging purposes.
	 */
	@Override
	public void init() 
	{
		if(debug)
		{
			System.out.println("=========================================");
			System.out.println("=========================================");
			System.out.println("===========  START OF PROGRAM  ==========");
			System.out.println("=========================================");
			System.out.println("=========================================\n\n");
			
			System.out.println("Entered function: init(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime) + " ms");
			System.out.println((this.getId() == 0) ? "I'm the sequencer\n" : "I'm not the sequencer\n" );
		}
		
		try
		{
			setup = SetupParser.parseFile("mcgui/localhostsetup");
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		
		if(debug)
		{
			System.out.println("Contents of setup file: ");
			for(String[] row : setup)
			{
				for(String column : row)
				{
					System.out.print(column + " ");
				}
				System.out.println();
			}
		}
		
		//We fill the hostList with the same information...
		hostList = new ArrayList<String[]>(setup);
		//But we remove index 0, to skip the "number of clients"-line.
		hostList.remove(0);
		
		numberOfClients = hostList.size();
		if(debug)
			System.out.println("\nnumberOfClients = " + numberOfClients);
		
		//We add 1 to our id to skip the first line in the setup file (which gives the number of hosts).
		String[] myInfo = setup.get(this.id+1);
		if(debug)
			System.out.println("me = " + myInfo[0]);
		
        myPort = Integer.parseInt(myInfo[1]);
        if(debug)
        	System.out.println("myport = " + myPort);

		
		if(debug)
			System.out.println("Our hostList: ");
		//We want each entry in the hostList to contain the host's ID (same as the index), hostname and port
		int index = 0;
		for(String[] host : hostList)
		{
			String hostName = host[0];
			String port = host[1];
			String[] newEntry = {String.valueOf(index), hostName, port};
			hostList.set(index, newEntry);
			
			if(debug)
				System.out.println("#" + hostList.get(index)[0] + "\nHostname: " + hostList.get(index)[1] + "\nPort: " + hostList.get(index)[2] + "\n");
			index++;
		}
		
		
	}

	/*
	 * Extended from the abstract class Multicaster.
	 * Gets called whenever the user presses the "cast"-button in the GUI.
	 * Will call the function basicsend() to actually send the entered message.
	 */
	@Override
	public void cast(String messagetext)
	{
		if(debug)
			System.out.println("Entered function: cast(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime) + " ms");
		
		//Create the message object to be sent to everyone.
		MCmessage message = new MCmessage(this.getId());
		
		//Set the message to be sent.
		message.setMessage(messagetext);
		
		//Set the timestamp to our logical clock value, will be used to detect failed transmissions and request retransmissions
		message.setTimestamp(logClock);
		
		//Set the message ID to be able to track it
		message.setMessageID(UUID.randomUUID().toString());
		
		//Add the hash to the message.
		message.setHash(produceHash(message));
		
		//Set the message type to NORMAL
		message.setType(NORMAL);
		
		//Each host saves the message, in case the sequencer requests a retransmission
		savedMsgList.add(message);
		
		//Send to the sequencer, which will set a timestamp and then commit it
		bcom.basicsend(sequencer, (Message) message);
		//
		message.setType(SHWNRM);
		//Send a copy to the shadow sequencer.
		bcom.basicsend(shadowSeq, (Message) message);
	}

	/*
	 * Extended from the abstract class Multicaster.
	 */
	@Override
	public void basicreceive(int peer, Message message)
	{
		if(debug)
			System.out.println("\nEntered function: basicreceive(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime) + " ms");
		
		MCmessage recvMsg = (MCmessage) message;
		
		//We have received a NORMAL message, so we are the sequencer and need to assign a timestamp.
		if(recvMsg.getType() == NORMAL)
		{	
			if(debug)
			{
				System.out.println("========== NORMAL ==================");
				System.out.println("Message   = " + recvMsg);
				System.out.println("From      = " + recvMsg.getsenderID());
				System.out.println("UUID      = " + recvMsg.getMessageID());
				System.out.println("Timestamp = " + recvMsg.getTimestamp());
				System.out.println("Hash      = " + recvMsg.getHash());
				System.out.println((checkHash(recvMsg.getHash(), recvMsg) ? "The hash is correct" : "The hash is wrong") );
				System.out.println("====================================\n");
			}
			
			//If the timestamp of the received message is too high, request a retransmission from the host
			if(recvMsg.getTimestamp() > vectorClock[recvMsg.getsenderID()] + 1)
			{
				requestResend(vectorClock[recvMsg.getsenderID()] + 1, recvMsg.getsenderID());
			}
			
			//Otherwise, we proceed as usual
			else
			{
				//Increase our vector clock
				vectorClock[recvMsg.getsenderID()] = recvMsg.getTimestamp() + 1;	
				
				sendCommit(recvMsg);
			
				//Put the message in our list of messages to be ACKed by the receiving nodes.
				ackList.put(recvMsg.getMessageID(), 0);
			}		
			

			if(debug)
				System.out.println("New vector clock == {" + vectorClock[0] + ", " + vectorClock[1] + ", " + vectorClock[2] + "}");
			
			if(debug)
				System.out.println("Number of unacknowledged messages: " + ackList.size());
			
		}
		//We have received an acknowledgement...
		else if(recvMsg.getType() == ACK)
		{
			ackList.put(recvMsg.getMessageID(), ackList.get(recvMsg.getMessageID()) + 1);
			if(debug)
			{
				System.out.println("========== ACK =====================");
				System.out.println("Message   = " + recvMsg);
				System.out.println("From      = " + recvMsg.getsenderID());
				System.out.println("UUID      = " + recvMsg.getMessageID());
				System.out.println("Timestamp = " + recvMsg.getTimestamp());
				System.out.println("Hash      = " + recvMsg.getHash());
				System.out.println((checkHash(recvMsg.getHash(), recvMsg) ? "The hash is correct" : "The hash is wrong") );
				System.out.println("The message now has " + ackList.get(recvMsg.getMessageID()) + "/" + hostList.size() + " ACKs");
				System.out.println("====================================");
			}
			
			//We check if this was the last needed ACK for a message.
			if(ackList.get(recvMsg.getMessageID()) == hostList.size())
			{
				if(debug)
					System.out.println("All ACKs received!");
				//When we have received all ACKs, remove it from the HashMap and ArrayList.
				ackList.remove(recvMsg.getMessageID());
			}
			
			//Increase our vector clock
			vectorClock[recvMsg.getsenderID()] = recvMsg.getTimestamp() + 1;
			
		}
		//The sequencer has sent us a COMMIT message
		else if(recvMsg.getType() == COMMIT)
		{
			if(debug)
			{
				System.out.println("========== COMMIT ==================");
				System.out.println("Message   = " + recvMsg);
				System.out.println("From      = " + recvMsg.getsenderID());
				System.out.println("UUID      = " + recvMsg.getMessageID());
				System.out.println("Timestamp = " + recvMsg.getTimestamp());
				System.out.println("Hash      = " + recvMsg.getHash());
				System.out.println((checkHash(recvMsg.getHash(), recvMsg) ? "The hash is correct" : "The hash is wrong") );
				System.out.println("====================================");
			}	
			//Check that the message is not ahead of us; if it is, then store it for later.
			if(recvMsg.getTimestamp() > lastDelivered+1)
			{
				if(debug)
					System.out.println("Can't deliver this message yet, delaying...");
				
				//We need another message first, before we can deliver this one, so we request a retransmission
				requestResend(lastDelivered+1, recvMsg.getsenderID());
				
				//Add the message to a queue for delivery later
				msgList.add(recvMsg);
			}
			//Check that it's not a message we have already delivered (to respect INTEGRITY)
			else if(recvMsg.getTimestamp() <= lastDelivered)
			{
				if(debug)
					System.out.println("Already delivered this message, ignoring...");
			}
			//Else, the message is the message we expected to receive, so we deliver it.
			else
			{
				if(debug)
					System.out.println("Expected message received, delivering...");
				
				//Update this whenever we deliver a message to keep track of where we are.
				lastDelivered++;
				
				this.mcui.deliver(recvMsg.getsenderID(), recvMsg.getTimestamp() + " " + recvMsg.toString());
				
				sendAck(recvMsg);					
				
				//If we are not the sequencer, we update our timestamp to stay up to date in case we have to become sequencer later.
				if(this.getId() != sequencer)
				{
					logClock = Math.max(recvMsg.getTimestamp(), logClock) + 1;
					
					//Increase our vector clock
					vectorClock[recvMsg.getsenderID()] = recvMsg.getTimestamp();
				}
					System.out.println("Updated my logClock to " + logClock);
			}
			
			if(debug)
				System.out.println("Checking msgList... lastDelivered = " + lastDelivered);
			//After we have delivered a message, and if our msgList is not empty,
			//see if we can deliver any of the saved messages.
			//We keep looping through the list until we have delivered all messages allowed (to respect VALIDITY).
			for(Iterator<MCmessage> it = msgList.iterator(); it.hasNext();)
			{
				//Iterate over all available messages
				MCmessage msg = it.next();
				if(debug)
					System.out.println("msg timestamp = " + msg.getTimestamp() + " message = " + msg.getMessage());
				if(msg.getTimestamp() == lastDelivered+1)
				{
					
					if(debug)System.out.println("Found a message to deliver, timestamp = " + msg.getTimestamp());
					this.mcui.deliver(msg.getsenderID(), msg.getTimestamp() + " " + msg.toString());
					it.remove();

					//Update this whenever we deliver a message to keep track of where we are.
					lastDelivered++;
					
					if(debug)
						System.out.println("Resetting iterator to start of list...");
					//Iterate over the msgList from the start again, after having removed a message.
					it = msgList.iterator();
				}
			}
		}
		//Some host has requested a RESEND of a message.
		else if(recvMsg.getType() == RESEND)
		{
			if(debug)
			{
				System.out.println("========== RESEND ==================");
				System.out.println("Message   = " + recvMsg);
				System.out.println("From      = " + recvMsg.getsenderID());
				System.out.println("UUID      = " + recvMsg.getMessageID());
				System.out.println("Timestamp = " + recvMsg.getTimestamp());
				System.out.println("Hash      = " + recvMsg.getHash());
				System.out.println((checkHash(recvMsg.getHash(), recvMsg) ? "The hash is correct" : "The hash is wrong") );
				System.out.println("====================================\n");
			}
			
			resendMsg(Integer.parseInt(recvMsg.getMessage()), recvMsg.getsenderID());
		}
		//The shadow sequencer has received a copy of the message to be saved. (SHadoWNoRMal)
		else if(recvMsg.getType() == SHWNRM)
		{
			shadowList.add(recvMsg);
		}
		
	}

	/**
	 * The "sender" has sent the sequencer a request to resend a message.
	 * We find the message in our savedMsgList and send it only to the requesting host.
	 */
	private void resendMsg(int msgTimestamp, int sender)
	{
		if(debug)
			System.out.println("Entered function: resendMsg(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime) + " ms");
		
		for(Iterator<MCmessage> listIterator = savedMsgList.iterator(); listIterator.hasNext();)
		{
			MCmessage msg = listIterator.next();
			
			if(msg.getTimestamp() == msgTimestamp)
			{
				System.out.println("Sending RESEND to " + sender);
				System.out.println("Message = " + msg.getMessage());
				System.out.println("Timestamp = " + msg.getTimestamp());
				System.out.println("Sender = " + msg.getSender());
				bcom.basicsend(sender, (Message) msg);
			}
		}
	}

	/**
	 * 
	 */
	private void requestResend(int requestedMsg, int target)
	{
		if(debug)
			System.out.println("Entered function: requestResend(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime) + " ms");
		
		//We need to ACKNOWLEDGE the message received
		MCmessage reqMsg = new MCmessage(this.getId());
		
		//Set the message type to ACK
		reqMsg.setType(RESEND);
		
		//Since it's just an acknowledgement, we don't need an actual message
		reqMsg.setMessage(Integer.toString(requestedMsg));
		
		//Timestamp does not matter
		reqMsg.setTimestamp(-1);
		
		//Create a new hash for the message.
		reqMsg.setHash(produceHash(reqMsg));
		
		System.out.println("Requesting a RESEND of message #" + requestedMsg);
		//Only the sequencer keeps track of the not-ACKnowledged messages.
		bcom.basicsend(target, (Message) reqMsg);
	}

	/**
	 * @param messageID
	 */
	private void removeSavedMsg(String messageID)
	{
		for(Iterator<MCmessage> listIterator = savedMsgList.iterator(); listIterator.hasNext();)
		{
			MCmessage msg = listIterator.next();
			
			if(msg.getMessageID() == messageID)
			{
				listIterator.remove();
			}
		}
	}

	private void sendCommit(MCmessage msg)
	{
		if(debug)
			System.out.println("Entered function: sendCommit(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime) + " ms");
		
		//We need to COMMIT the message received
		MCmessage comMsg = new MCmessage(this.getId());
		//Set the message type to COMMIT
		comMsg.setType(COMMIT);
		
		//The sequencer sets the timestamp on the message.
		comMsg.setTimestamp(this.logClock++);
		
		//Use the same values as the original message
		comMsg.setsenderID(msg.getsenderID());
		comMsg.setMessageID(msg.getMessageID());
		comMsg.setMessage(msg.getMessage());
		
		//Produce a new hash for the new message, to make sure it was received correctly.
		comMsg.setHash(produceHash(comMsg));
		
		//Also store the whole message in a separate list
		savedMsgList.add(comMsg);
		
		try
		{
			int i = 0;
			//Send to all hosts the message to be delivered.
			for(String[] host : hostList)
			{				
				//Send to everyone, including ourselves
				if(debug)
					System.out.println("Sending COMMIT to peer #" + i);
				//Send the message. Arguments are the host-ID of the intended recipient and the message.
				bcom.basicsend(Integer.parseInt(host[0]), (Message) comMsg);
				
				i++;
			}
		} catch (Exception e){e.printStackTrace();}
	}

	private void sendAck(MCmessage msg)
	{
		if(debug)
			System.out.println("Entered function: sendAck(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime) + " ms");
		
		//We need to ACKNOWLEDGE the message received
		MCmessage ackMsg = new MCmessage(this.getId());
		
		//Set the message type to ACK
		ackMsg.setType(ACK);
		
		//Set the timestamp and message id of the message to be the same as the original.
		ackMsg.setTimestamp(logClock);
		ackMsg.setMessageID(msg.getMessageID());
		
		//Since it's just an acknowledgement, we don't need an actual message
		ackMsg.setMessage("");
		
		//Create a new hash for the message.
		ackMsg.setHash(produceHash(ackMsg));
		
		//Only the sequencer keeps track of the not-ACKnowledged messages.
		bcom.basicsend(sequencer, (Message) ackMsg);
		
	}

	//Extended from the abstract class Multicaster
	@Override
	public void basicpeerdown(int peer)
	{
		if(debug)
			System.out.println("Entered function: basicpeerdown(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime) + " ms");		
		
		// We iterate over our list of hosts with an Iterator. 
		// When we find the right host, we remove it from the list.
		for(Iterator<String[]> listIterator = hostList.iterator(); listIterator.hasNext();)
		{
			String[] host = listIterator.next();
			
			if(Integer.parseInt(host[0]) == peer)
			{
				if(debug)
					System.out.println("Detected a crash, removing host #" + peer);
				listIterator.remove();
				
				if(peer == sequencer)
				{
					if(debug)
						System.out.println("Crashed host was the sequencer, select a new one");
					selectNewSeq(peer);
				}
			}
		}
	}
	
	
	
	private void selectNewSeq(int peer)
	{
		if(debug)
			System.out.println("Entered function: selectNewSeq(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime) + " ms");

		if(hostList.size() > 0)
		{
			//Make sure our hostList is sorted according to host IDs...
			Collections.sort(hostList, new Comparator<String[]>()
			{
				@Override
				public int compare(String[] str1, String[] str2){return str1[0].compareTo(str2[0]);}}
			);
		
			if(debug)
			{
				System.out.println("Contents of hostList after custom sort: ");
				for(String[] host : hostList)
				{
					System.out.println("#" + host[0] + "\nHostname: " + host[1] + "\nPort: " + host[2] + "\n");
				}
			}
			
			//And then choose the next sequencer according to IDs sorted in ascending order
			sequencer = Integer.parseInt(hostList.get(0)[0]);
			
			//As long as we have two or more hosts available, also select a new shadow sequencer.
			if(hostList.size() > 1)
			{
				shadowSeq = Integer.parseInt(hostList.get(1)[0]);;
			}
			else
			{
				shadowSeq = -1;
			}
			if(debug)
				System.out.println(sequencer==this.getId() ? "The sequencer is now me" : "The sequencer is now " + sequencer);
			//If we are the new sequencer, replay our list of saved messages
			if(this.getId() == sequencer)
			{
				//Limit the amount of messages to replay
				for(Iterator<MCmessage> listIterator = shadowList.listIterator(shadowList.size() / 2); listIterator.hasNext();)
				{
					MCmessage msg = listIterator.next();
					System.out.println("Sending message == " + msg.getMessage());
					for(Iterator<String[]> iterator = hostList.iterator(); iterator.hasNext();)
					{
						String[] host = iterator.next();
						
						//Send the message to all hosts
						bcom.basicsend(Integer.parseInt(host[0]), (Message) msg);
					}
				}
			}
			//After we have replayed the messages, clear the list
			shadowList.clear();
		}
		else
		{
			if(debug)
				System.out.println("No more hosts...");
		}
		
		
	}

	
	@Override
	public void deliver(int from, String message, String info)
	{
	}
	

	@Override
	public void deliver(int from, String message)
	{
	}

	@Override
	public void debug(String string)
	{
	}

	@Override
	public void enableSending()
	{
	}
	

	private byte[] produceHash(MCmessage message)
	{
		if(extensiveDebug)
			System.out.println("Entered function: produceHash(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime) + " ms");
		
		//Will be used for creating a SHA-256 hash of the message to be sent
		MessageDigest md = null;
		
		try
		{
			md = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e){e.printStackTrace();}
		
		//Create a SHA-256 hash of the message + host-ID + timestamp.
		md.update((message.getMessage()+message.getsenderID()+message.getTimestamp()).getBytes());
		
		return md.digest();
	}
	//Check if the received hash is the same as the calculated one
	public Boolean checkHash(byte[] msgHash, MCmessage msg)
	{
		if(extensiveDebug)
			System.out.println("Entered function: checkHash(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime) + " ms");
		return MessageDigest.isEqual(msgHash, produceHash(msg));
	}

}

/*
class repeatingTimer extends Timer
{
	private Runnable task;
	private TimerTask timerTask;
	
	public void schedule(Runnable runnable, long delay)
	{
		task = runnable;
		
		timerTask = new TimerTask(){ public void run() { task.run(); }};
	}
	
	public void reschedule(long delay)
	{
		//Cancel the running timer...
		timerTask.cancel();
		
		//And make a new one.
		timerTask = new TimerTask(){ public void run() { task.run(); }};
		
	}
}*/
