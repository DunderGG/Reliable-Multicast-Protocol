package mcgui;

/*
 * 		AUTHOR: 	David Bennehag (David.Bennehag@Gmail.com)
 * 		VERSION: 	1.0
 * 
 * 		COURSE: Distributed Systems, Advanced (MPCSN, Chalmers University of Technology)
 *		
 *		TODO
 *
 * 		FINISHED
 * 
 */


@SuppressWarnings("serial")
public class MCmessage extends Message 
{
	//Only used for debugging purposes
	private boolean debug;
	
	private String message;
	private int type;
	private String messageID;
	private int timestamp;
	private byte[] hash;
	
	private int acks;
	
	/*
	 * Constructor
	 */
	public MCmessage(int sender) 
	{
		super(sender);
		
		//For debugging purposes
		debug = false;
	
		if(debug)
			System.out.println("Constructing a new MCmessage...\n");

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
	
	public int getsenderID()
	{
		return sender;
	}
	public void setsenderID(int id)
	{
		this.sender = id;
	}

	public String getMessageID()
	{
		return messageID;
	}
	public void setMessageID(String messageID)
	{
		this.messageID = messageID;
	}
	
	public byte[] getHash()
	{
		return hash;
	}
	public void setHash(byte[] hash)
	{
		this.hash = hash;
	}
	
	public int getType()
	{
		return type;
	}
	public void setType(int type)
	{
		this.type = type;
	}

	public int getAcks()
	{
		return acks;
	}

	public void setAcks(int acks)
	{
		this.acks = acks;
	}
}
