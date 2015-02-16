package mcgui;
import mcgui.*;

import java.io.IOException;
import java.util.ArrayList;

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

public class MCmodule extends Multicaster implements BasicCommunicator
{

	public MCmodule()
	{
		System.out.println("Hello World!\n");
	}
	
	//Extended from the abstract class Multicaster
	//Used to initialize stuff and do some basic printing for debugging purposes
	@Override
	public void init() 
	{
		System.out.println("Entered function: init(), I am id #" + this.getId());
		
		ArrayList<String[]> setup = null;
		try
		{
			setup = SetupParser.parseFile("mcgui\\localhostsetup");
			System.out.println();
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
			System.out.println();
			
			int numberOfClients = Integer.parseInt(setup.get(0)[0]);
			System.out.println("numberOfClients = " + numberOfClients);
			
			//We add 1 to our id to skip the first line in the setup file, which gives the number of hosts
			String[] myInfo = setup.get(this.id+1);
			System.out.println("me = " + myInfo[0]);
			
            int myport = Integer.parseInt(myInfo[1]);
            System.out.println("myport = " + myport);
            
            System.out.println("Finished initializing, moving on...\n");
            
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//Implemented from the interface BasicCommunicator
	//Will be used for sending a message to a specific receiver
	@Override
	public void basicsend(int receiver, Message message)
	{
		System.out.println("Entered function: basicsend(), I am id #" + this.getId());

		System.out.println("receiver: " + receiver);
		System.out.println("message: " +((MCmessage) message).getMessage());
	}
	
	

	//Extended from the abstract class Multicaster
	//Gets called whenever the user presses the "cast"-button in the GUI
	//Will call the function basicsend() to actually send the entered message
	@Override
	public void cast(String messagetext)
	{

		System.out.println("Entered function: cast(), I am id #" + this.getId() + "\n");
		int receiver = this.getId();
		
		MCmessage message = new MCmessage(receiver, messagetext);
		
		basicsend(receiver, (Message)message);
	}

	//Extended from the abstract class Multicaster
	@Override
	public void basicreceive(int peer, Message message)
	{
		
	}

	//Extended from the abstract class Multicaster
	@Override
	public void basicpeerdown(int peer)
	{
		
	}


}
