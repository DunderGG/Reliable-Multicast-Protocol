package mcgui;
import mcgui.*;

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
		System.out.println("Hello World!");
	}
	
	//Implemented from the interface BasicCommunicator
	//Will be used for sending a message to a specific receiver
	@Override
	public void basicsend(int receiver, Message message)
	{
		System.out.println("Entered function: basicsend()");
		System.out.println("receiver: " + receiver);
		System.out.println("message: " +((MCmessage) message).getMessage());
	}
	
	//Extended from the abstract class Multicaster
	@Override
	public void init() 
	{
		
	}

	//Extended from the abstract class Multicaster
	//Gets called whenever the user presses the "cast"-button in the GUI
	@Override
	public void cast(String messagetext)
	{
		System.out.println("Entered function: cast()");
		int receiver = 0;
		
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
