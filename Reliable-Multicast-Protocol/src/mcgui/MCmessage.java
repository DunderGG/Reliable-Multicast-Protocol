package mcgui;

/*
 * AUTHOR: 	David Bennehag (David.Bennehag@Gmail.com)
 * Version: 1.0
 * 
 * 		Distributed Systems, Advanced (MPCSN, Chalmers University of Technology)
 *		
 *		TODO:  
 * 
 */

@SuppressWarnings("serial")
public class MCmessage extends Message 
{
	private String message;
	private int id;
	private int timestamp;
	
	//sender must, according to specification, be initialized before the object is used
	public MCmessage(int sender) 
	{
		super(sender);
	
		System.out.println("Entered Constructor for MCmessage\n");

	}
	@Override
	public String toString()
	{
		return message;
	}

	public String getMessage() 
	{
		return message;
	}
	public void setMessage(String message) 
	{
		this.message = message;
	}
	
	public int getTimestamp()
	{
		return timestamp;
	}
	public void setTimestamp(int timestamp)
	{
		this.timestamp = timestamp;
	}
	
	public int getId()
	{
		return id;
	}
	public void setId(int id)
	{
		this.id = id;
	}

}
