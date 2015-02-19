package mcgui;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;

/*
 * AUTHOR: 	David Bennehag (David.Bennehag@Gmail.com)
 * Version: 1.0
 * 
 * 		Distributed Systems, Advanced (MPCSN, Chalmers University of Technology)
 *
 *		SOURCE: 	E:\Program\Git\repository\Reliable-Multicast-Protocol\Reliable-Multicast-Protocol\src
 *		
 *		RUN WITH: 	java mcgui.Main mcgui.MCmodule 1 mcgui\localhostsetup		
 *
 *		TODO: Implement a reliable and ordered multicast, driven by a provided GUI and pre-setup TCP connections.
 *				Needs to cope with crashing processes, but not joining processes. 
 * 
 */

public class MCmodule extends Multicaster implements MulticasterUI
{
	//Different types of messages
	public static final int NORMAL = 0;
	public static final int ACK = 1;
	public static final int COMMIT = 2;
	
	
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
	
	//Will contain the same information, but skip the first line
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
		numberOfClients = 0;
		myPort = 0;
		logClock = 0;
		
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
		System.out.println("=========================================");
		System.out.println("=========================================");
		System.out.println("===========  START OF PROGRAM  ==========");
		System.out.println("=========================================");
		System.out.println("=========================================\n\n");
		
		System.out.println("Entered function: init(), I am id #" + this.getId() + "\n");
		
		try
		{
			setup = SetupParser.parseFile("mcgui\\localhostsetup");

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
			//We fill the hostList with the same information...
			hostList = new ArrayList<String[]>(setup);
			//But we remove index 0, to skip the "number of clients"-line.
			hostList.remove(0);
			
			numberOfClients = Integer.parseInt(setup.get(0)[0]);
			System.out.println("\nnumberOfClients = " + numberOfClients);
			
			//We add 1 to our id to skip the first line in the setup file (which gives the number of hosts).
			String[] myInfo = setup.get(this.id+1);
			System.out.println("me = " + myInfo[0]);
			
            myPort = Integer.parseInt(myInfo[1]);
            System.out.println("myport = " + myPort);

            System.out.println("Finished initializing, moving on...\n");
            
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		
		communicator.setMulticaster(this);
		
		try
		{
			//Connect to each host
			for(String[] host : hostList)
			{				
				System.out.println("Connecting to host " + host[0] + " on port " + host[1]);
				communicator.connect(this.getId(), host[0], Integer.parseInt(host[1]));
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
		System.out.println("Entered function: cast(), I am id #" + this.getId() + " and my port is " + this.myPort + "\n");
		
		//Create the message object to be sent to everyone.
		MCmessage message = new MCmessage(this.getId());
		
		//Set the message to be sent.
		message.setMessage(messagetext);
		
		//Set the timestamp of the message and then increment our clock value.
		message.setTimestamp(logClock++);
		
		
		//Set the message ID to be able to track it
		message.setMessageID(UUID.randomUUID().toString());
		
		//Add the hash to the message.
		message.setHash(produceHash(message));
		
		//Set the message type to NORMAL
		message.setType(NORMAL);
		
		//Add the message to our list for future handling.
		msgList.add(message);
		
		//Put the message in our list of messages to be ACKed
		ackList.put(message.getMessageID(), 0);
		System.out.println("Number of unacknowledged messages: " + ackList.size());
		
		try
		{
			int i = 0;
			//Send to each host.
			for(String[] host : hostList)
			{				

				//Send to everyone, including ourselves
				System.out.println("\nSending to id: " + i);				
				System.out.println("Sending message: " + message);
				System.out.println("Message has timestamp: " + message.getTimestamp());
				//Send the message. Arguments are the host-ID of the intended recipient and the message.
				communicator.basicsend(i, (Message) message);
				
				i++;
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		
			
	}

	/*
	 * Extended from the abstract class Multicaster.
	 */
	@Override
	public void basicreceive(int peer, Message message)
	{
		System.out.println("\nEntered function: basicreceive(), I am id #" + this.getId());
		
		MCmessage recvMsg = (MCmessage) message;
		
		System.out.println("type = " + recvMsg.getType());
		
		//We have received a regular message which we need to acknowledge
		if(recvMsg.getType() == 0)
		{
			if(recvMsg.getsenderID() != this.getId())
			{
				System.out.println("========== NORMAL ==================");
				System.out.println("Message   = " + recvMsg);
				System.out.println("From      = " + recvMsg.getsenderID());
				System.out.println("UUID      = " + recvMsg.getMessageID());
				System.out.println("Timestamp = " + recvMsg.getTimestamp());
				System.out.println("Hash      = " + recvMsg.getHash());
				System.out.println((checkHash(recvMsg.getHash(), recvMsg) ? "The hash is correct" : "The hash is wrong") );
				System.out.println("====================================\n");
			
				msgList.add(recvMsg);
				
				if(recvMsg.getTimestamp() >= logClock)
				{
					logClock = recvMsg.getTimestamp() + 1;
				}
				
			}
			//Send an ACK to the host who sent us a message
			System.out.println("Sending ACK...");
			sendAck(recvMsg);
		}
		//We have received an acknowledgement
		else if(recvMsg.getType() == 1)
		{
			ackList.put(recvMsg.getMessageID(), ackList.get(recvMsg.getMessageID()) + 1);
			
			System.out.println("========== ACK =====================");
			System.out.println("Message   = " + recvMsg);
			System.out.println("From      = " + recvMsg.getsenderID());
			System.out.println("UUID      = " + recvMsg.getMessageID());
			System.out.println("Timestamp = " + recvMsg.getTimestamp());
			System.out.println("Hash      = " + recvMsg.getHash());
			System.out.println((checkHash(recvMsg.getHash(), recvMsg) ? "The hash is correct" : "The hash is wrong") );
			System.out.println("The message now has " + ackList.get(recvMsg.getMessageID()) + "/" + hostList.size() + " ACKs");
			System.out.println("====================================");
			
			if(ackList.get(recvMsg.getMessageID()) == hostList.size())
			{
				System.out.println("All ACKs have now been received for message: " + recvMsg.getMessageID());
				
				//TODO: Before delivering, we must make sure everyone agrees on the message ordering
				
				//We iterate over our list of messages with an Iterator. 
				// When we find the right message, we deliver it and then remove it from the list.
				for(Iterator<MCmessage> it = msgList.iterator(); it.hasNext();)
				{
					MCmessage msg = it.next();
					
					if(msg.getMessageID().equals(recvMsg.getMessageID()))
					{
						
						it.remove();
						
						//Also remove the key from our HashMap of messages that needs acknowledgement
						ackList.remove(msg.getMessageID());
						System.out.println("Number of unacknowledged messages: " + ackList.size());
						
						System.out.println("Sending COMMIT...");
						sendCommit(msg);
					}
				}
			}			
		}
		//The original sender has received all ACKs and has now sent us a COMMIT message
		else if(recvMsg.getType() == 2)
		{
			System.out.println("========== COMMIT ==================");
			System.out.println("Message   = " + recvMsg);
			System.out.println("From      = " + recvMsg.getsenderID());
			System.out.println("UUID      = " + recvMsg.getMessageID());
			System.out.println("Timestamp = " + recvMsg.getTimestamp());
			System.out.println("Hash      = " + recvMsg.getHash());
			System.out.println((checkHash(recvMsg.getHash(), recvMsg) ? "The hash is correct" : "The hash is wrong") );
			System.out.println("The message now has " + ackList.get(recvMsg.getMessageID()) + "/" + hostList.size() + " ACKs");
			System.out.println("====================================");
			
			this.mcui.deliver(recvMsg.getsenderID(), recvMsg.getTimestamp() + " " + recvMsg.toString(), Integer.toString(recvMsg.getTimestamp()));
		}
		
	}

	private void sendCommit(MCmessage msg)
	{
		//We need to COMMIT the message received
		MCmessage comMsg = new MCmessage(this.getId());
		//Set the message type to COMMIT
		comMsg.setType(COMMIT);
		
		comMsg.setMessageID(msg.getMessageID());
		
		comMsg.setMessage(msg.getMessage());
		
		comMsg.setHash(msg.getHash());
		
		
		
		try
		{
			int i = 0;
			//Send to each host.
			for(String[] host : hostList)
			{				
				//Send to everyone, including ourselves

				//Send the message. Arguments are the host-ID of the intended recipient and the message.
				communicator.basicsend(i, (Message) comMsg);
				
				i++;
			}
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private void sendAck(MCmessage msg)
	{
		//We need to ACKNOWLEDGE the message received
		
		MCmessage ackMsg = new MCmessage(this.getId());
		
		ackMsg.setMessageID(msg.getMessageID());
		
		//Set the timestamp of the message and then increment our clock value.
		ackMsg.setTimestamp(logClock);
		
		//Add the hash to the message.
		ackMsg.setHash(produceHash(ackMsg));
		
		//Set the message type to ACK
		ackMsg.setType(ACK);
		
		communicator.basicsend(msg.getsenderID(), (Message) ackMsg);
	}

	//Extended from the abstract class Multicaster
	@Override
	public void basicpeerdown(int peer)
	{
		System.out.println("Entered function: basicpeerdown(), I am id #" + this.getId() + "\n");

		//We remove the crashed peer from our list of hosts 
		hostList.remove(peer);
	}
	
	
	
	/*
	 * 	Implemented from the interface MulticasterUI
	 * 	ARGUMENTS:
	 * 		from 		= the host we received the message from
	 * 		message 	= the message contained inside
	 * 		info 		= right now it's the timestamp
	 */
	@Override
	public void deliver(int from, String message, String info)
	{
		// TODO Auto-generated method stub
		
		
		System.out.println("Delivered message from peer #" + from + "\n");
	}
	
	/*
	 * Implemented from the interface MulticasterUI
	 */
	@Override
	public void deliver(int from, String message)
	{
		// TODO Auto-generated method stub
		
	}
	/*
	 * Implemented from the interface MulticasterUI
	 */
	@Override
	public void debug(String string)
	{
		// TODO Auto-generated method stub
		
	}
	/*
	 * Implemented from the interface MulticasterUI
	 */
	@Override
	public void enableSending()
	{
		// TODO Auto-generated method stub
		
	}

	private byte[] produceHash(MCmessage message)
	{
		//Will be used for creating a SHA-256 hash of the message to be sent
		MessageDigest md = null;
		
		try
		{
			md = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//Create a SHA-256 hash of the message + host-ID + timestamp.
		md.update( (message.getMessage()+message.getsenderID()+message.getTimestamp()).getBytes() );
		
		byte[] msgHash = md.digest();
		
		return msgHash;
	}
	//Check if the received hash is the same as the calculated one
	public Boolean checkHash(byte[] msgHash, MCmessage msg)
	{
		
		byte[] digestb = produceHash(msg);
		
		return MessageDigest.isEqual(msgHash, digestb);
	}

}
