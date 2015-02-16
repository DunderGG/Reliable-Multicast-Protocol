package mcgui;
import mcgui.*;

/*
 * AUTHOR: 	David Bennehag (David.Bennehag@Gmail.com)
 * Version: 1.0
 * 
 * 		Distributed Systems, Advanced (MPCSN, Chalmers University of Technology)
 *		
 *		TODO:  
 * 
 */

public class MCmessage extends Message 
{
	private String message;
	
	//sender must, according to specification, be initialized before the object is used
	public MCmessage(int sender, String message) 
	{
		super(sender);
		
		// TODO
		this.setMessage(message);
	}

	public String getMessage() 
	{
		return message;
	}

	public void setMessage(String message) 
	{
		this.message = message;
	}

}
