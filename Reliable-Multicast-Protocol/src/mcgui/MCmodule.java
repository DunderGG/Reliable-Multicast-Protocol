package mcgui;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;

/*
 * AUTHOR: 	David Bennehag (David.Bennehag@Gmail.com)
 * Version: 1.0
 * 
 * 		Distributed Systems, Advanced (MPCSN, Chalmers University of Technology)
 *		
 *
 *		TODO: Implement a reliable and ordered multicast, driven by a provided GUI and pre-setup TCP connections.
 *				Needs to cope with crashing processes, but not joining processes. 
 * 
 */

public class MCmodule extends Multicaster implements MulticasterUI
{
	private ArrayList<String[]> setup;
	private int numberOfClients, myport;
	private int clockValue;
	TCPCommunicator communicator;
	
	private ArrayList<MCmessage> msgList;

	/*
	 * 
	 */
	public MCmodule()
	{
		System.out.println("Entered Constructor for MCmodule\n");
		setup = null;
		numberOfClients = 0;
		myport = 0;
		clockValue = 0;
		msgList = new ArrayList<MCmessage>();
		communicator = new TCPCommunicator();
	}
	
	/*
	 * Extended from the abstract class Multicaster.
	 * Used to initialize stuff and do some basic printing for debugging purposes.
	 */
	@Override
	public void init() 
	{
		System.out.println("Entered function: init(), I am id #" + this.getId() + "\n");
		
		try
		{
			setup = SetupParser.parseFile("mcgui/localhostsetup");

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
			
			numberOfClients = Integer.parseInt(setup.get(0)[0]);
			System.out.println("\nnumberOfClients = " + numberOfClients);
			
			//We add 1 to our id to skip the first line in the setup file, which gives the number of hosts.
			String[] myInfo = setup.get(this.id+1);
			System.out.println("me = " + myInfo[0]);
			
            myport = Integer.parseInt(myInfo[1]);
            System.out.println("myport = " + myport);

            System.out.println("Finished initializing, moving on...\n");
            
		} catch (IOException e)
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
		System.out.println("Entered function: cast(), I am id #" + this.getId() + " and my port is " + this.myport + "\n");
		
		//Try sending the message to all the other hosts.
		try
		{
			
			communicator.setMulticaster(this);
			
			//Connect to each host.
			for(String[] host : setup)
			{				
				if(host.length == 2)
				{
					System.out.println("Connecting to host: " + host[0] + " on port: " + host[1]);
					communicator.connect(this.getId(), host[0], Integer.parseInt(host[1]));
				}
			}
			
			//Create the message object to be sent to everyone.
			MCmessage message = new MCmessage(myport);
			
			//Set the message to be sent.
			message.setMessage(messagetext);
			
			//Set the timestamp of the message and then increment our clock value.
			message.setTimestamp(clockValue++);
			
			//Set the peer id for the message, to be used for deliver().
			message.setId(this.getId());
			
			//Add the message to our list for future handling.
			msgList.add(message);
			
			//Send to each host.
			for(int i = 0; i < numberOfClients; i++)
			{				
				if(i != this.getId())
				{
					System.out.println("Sending to id: " + i);				
					System.out.println("Sending message: " + message);
					System.out.println("Message has timestamp: " + message.getTimestamp());
					//Call basicsend from BasicCommunicator; arguments are the id and the message.
					communicator.basicsend(i, (Message) message);
				}
			}
			//Don't deliver right away, we want to make sure we are synchronized with the others first
			//this.mcui.deliver(this.getId(), " " + message.toString());
			
		} catch (NumberFormatException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
	 * Extended from the abstract class Multicaster.
	 */
	@Override
	public void basicreceive(int peer, Message message)
	{
		MCmessage recvMsg = (MCmessage) message;
		System.out.println("Entered function: basicreceive(), I am id #" + this.getId());
		
		System.out.println("Received message: " + recvMsg);
		System.out.println("Message was sent from peer: " + recvMsg.getId());
		System.out.println("Message had timestamp: " + recvMsg.getTimestamp());
		
		//Before delivering, we must make sure everyone agrees on the message ordering
		this.mcui.deliver(recvMsg.getId(), " " + message.toString());
		System.out.println("Delivered message from peer #" + recvMsg.getId() + " with timestamp " + recvMsg.getTimestamp() + "\n");
	}

	//Extended from the abstract class Multicaster
	@Override
	public void basicpeerdown(int peer)
	{
		System.out.println("Entered function: basicpeerdown(), I am id #" + this.getId() + "\n");
	}
	
	
	
	/*
	 * Implemented from the interface MulticasterUI
	 */
	@Override
	public void deliver(int from, String message, String info)
	{
		// TODO Auto-generated method stub
		
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


}
