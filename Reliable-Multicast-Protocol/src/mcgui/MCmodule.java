package mcgui;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
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
 *		RUN WITH: 	java mcgui.Main mcgui.MCmodule 1 mcgui\localhostsetup		
 *
 *		TODO  
 *				Implement a reliable and ordered multicast, driven by a provided GUI and pre-setup TCP connections.
 *				Needs to cope with crashing processes, but not re-joining processes.
 *
 * 
 * 		FINISHED
 * 			Total Ordering is now fixed, but might need some refinement as it's very "raw" with only the most basic logic included.
 * 
 * 			Lowest ID (ID 0, first in the file) is the sequencer in the start. 
 * 				New sequencers are then chosen in ascending order.
 * 
 */

public class MCmodule extends Multicaster implements MulticasterUI
{
	//Only used for debugging purposes
	private long startTime;
	private boolean debug;
	
	//Different types of messages
	private static final int NORMAL = 0;
	private static final int ACK = 1;
	private static final int COMMIT = 2;
	
	//Will keep track of the current sequencer
	private int sequencer;
	
	//Will keep track of which message was last delivered, so we don't deliver a message too early or more than once.
	private int lastDelivered;
	
	//Pretty self-descriptive....
	private int numberOfClients, myPort;
	
	//Our logical clock so we can add timestamps to messages
	private int logClock;
	
	//The object to be used for communication with other hosts
	TCPCommunicator communicator;

	//Will be a list containing messages
	private ArrayList<MCmessage> msgList;
	
	//Will contain the contents of the setup file
	private ArrayList<String[]> setup;
	
	//Will contain ID, hostname and port
	private ArrayList<String[]> hostList;
	
	//Whenever we send a NORMAL message, we need to collect ACKs from the other nodes.
	//	The ACKs are stored in this ackList. 
	//	The key will be the messageID and the value is number of ACKs received
	private HashMap<String, Integer> ackList = new HashMap<String, Integer>();

	/*
	 * 
	 */
	public MCmodule()
	{		
		//For debugging purposes
		startTime = System.currentTimeMillis();
		debug = true;
		
		//Initialize our variables
		numberOfClients = 0;
		myPort = 0;
		logClock = 0;
		sequencer = 0;
		lastDelivered = -1;
				
		msgList = new ArrayList<MCmessage>();
		setup = new ArrayList<String[]>();
		
		communicator = new TCPCommunicator();
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
			
			System.out.println("Entered function: init(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime));
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
					System.out.print(column);
					System.out.print(" ");
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

		communicator.setMulticaster(this);
		
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

		try
		{
			//Connect to each host
			for(String[] host : hostList)
			{				
				if(debug)
					System.out.println("Connecting to host " + host[0] + " on port " + host[2]);
				communicator.connect(this.getId(), host[1], Integer.parseInt(host[2]));
			}
		} catch (NumberFormatException e)
		{
			e.printStackTrace();
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
			System.out.println("Entered function: cast(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime));
		
		//Create the message object to be sent to everyone.
		MCmessage message = new MCmessage(this.getId());
		
		//Set the message to be sent.
		message.setMessage(messagetext);
		
		//Set the timestamp of the message to -1 as the sequencer handles the timestamps on messages
		message.setTimestamp(-1);
		
		//Set the message ID to be able to track it
		message.setMessageID(UUID.randomUUID().toString());
		
		//Add the hash to the message.
		message.setHash(produceHash(message));
		
		//Set the message type to NORMAL
		message.setType(NORMAL);
		
		//Only send to the sequencer, which will set a timestamp and then commit it
		communicator.basicsend(sequencer, (Message) message);
	}

	/*
	 * Extended from the abstract class Multicaster.
	 */
	@Override
	public void basicreceive(int peer, Message message)
	{
		if(debug)
			System.out.println("\nEntered function: basicreceive(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime));
		
		MCmessage recvMsg = (MCmessage) message;
		
		//We have received a NORMAL message, so we are the sequencer and need to assign a timestamp.
		if(recvMsg.getType() == 0)
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
		
			sendCommit(recvMsg);
			
			//Put the message in our list of messages to be ACKed by the receiving nodes.
			ackList.put(recvMsg.getMessageID(), 0);
			if(debug)
				System.out.println("Number of unacknowledged messages: " + ackList.size());
		}
		//We have received an acknowledgement...
		else if(recvMsg.getType() == 1)
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
			  /////////////////////////////////
			 //SOME CODE STORED IN MCMODULE2//
			/////////////////////////////////
			
			//When we receive an ACK, update our ACK list.
			ackList.put(recvMsg.getMessageID(), ackList.get(recvMsg.getMessageID()) + 1);
			
			//We check if this was the last needed ACK for a message.
			if(ackList.get(recvMsg.getMessageID()) == hostList.size())
			{
				//When we have received all ACKs, remove it from the list.
				ackList.remove(recvMsg.getMessageID());
			}
			
		}
		//The sequencer has sent us a COMMIT message
		else if(recvMsg.getType() == 2)
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
				msgList.add(recvMsg);
			}
			//Check that it's not a message we have already delivered (to respect INTEGRITY)
			else if(recvMsg.getTimestamp() <= lastDelivered)
			{
				if(debug)
					System.out.println("Already delivered this message, ignoring...");
			}
			//If the message is not ahead of us, deliver it.
			else
			{
				if(debug)
					System.out.println("Correct message received, delivering...");
				lastDelivered++;
				this.mcui.deliver(recvMsg.getsenderID(), recvMsg.getTimestamp() + " " + recvMsg.toString());
				
				sendAck(recvMsg);
				
				//If we are not the sequencer, we update our logical clock to stay up to date.
				if(this.getId() != sequencer)
					logClock = Math.max(recvMsg.getTimestamp(), logClock) + 1;
				if(debug)
					System.out.println("Updated our logical clock to: " + logClock);
				
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
					System.out.println("msg timestamp = " + msg.getTimestamp() + "message = " + msg.getMessage());
				if(msg.getTimestamp() == lastDelivered+1)
				{
					
					if(debug)System.out.println("Found a message to deliver, timestamp = " + msg.getTimestamp());
					this.mcui.deliver(msg.getsenderID(), msg.getTimestamp() + " " + msg.toString());
					it.remove();

					if(debug)
						System.out.println("Resetting iterator to start of list...");
					//Iterate over the msgList from the start again, after having removed a message.
					it = msgList.iterator();
				}
			}
		}
		
	}

	private void sendCommit(MCmessage msg)
	{
		if(debug)
			System.out.println("Entered function: sendCommit(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime));
		
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
				communicator.basicsend(Integer.parseInt(host[0]), (Message) comMsg);
				
				i++;
			}
		} catch (Exception e){e.printStackTrace();}
	}

	private void sendAck(MCmessage msg)
	{
		if(debug)
			System.out.println("Entered function: sendAck(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime));
		
		//We need to ACKNOWLEDGE the message received
		MCmessage ackMsg = new MCmessage(this.getId());
		
		//Set the message type to ACK
		ackMsg.setType(ACK);
		
		//Set the timestamp and message id of the message to be the same as the original.
		ackMsg.setTimestamp(msg.getTimestamp());
		ackMsg.setMessageID(msg.getMessageID());
		
		//Since it's just an acknowledgement, we don't need an actual message
		ackMsg.setMessage("");
		
		//Create a new hash for the message.
		ackMsg.setHash(produceHash(ackMsg));
		
		//Only the sequencer keeps track of the not-ACKnowledged messages.
		communicator.basicsend(sequencer, (Message) ackMsg);
		
	}

	//Extended from the abstract class Multicaster
	@Override
	public void basicpeerdown(int peer)
	{
		if(debug)
			System.out.println("Entered function: basicpeerdown(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime));		
		
		//We iterate over our list of messages with an Iterator. 
		// When we find the right message, we deliver it and then remove it from the list.
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
			System.out.println("Entered function: selectNewSeq(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime));

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
		}
		else
		{
			if(debug)
				System.out.println("No more hosts...");
		}
		
		if(debug)
			System.out.println(sequencer==this.getId() ? "The sequencer is now me" : "The sequencer is now " + sequencer);
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
		if(debug)
			System.out.println("Entered function: produceHash(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime));
		
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
		if(debug)
			System.out.println("Entered function: checkHash(), I am id #" + this.getId() + "\nAt time: " + (System.currentTimeMillis() - startTime));
		return MessageDigest.isEqual(msgHash, produceHash(msg));
	}

}
