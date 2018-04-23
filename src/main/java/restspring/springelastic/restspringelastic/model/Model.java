package restspring.springelastic.restspringelastic.model;



public class Model {
	
	private String sentiment;
	private String image;
	private String user_id;
	private String tweet_id;
	private String screen_name;
	private String name;
	private String created_date;
	private String tweet_text;
	private String timestamp;

	public Model(String name, String user_id) {
	this.user_id = user_id;
	}


	public String getName() {
	return name;
	}
	public void setName() {
	this.name =name;
	}

	public String getUser_id() {
	return user_id;
	}
	public void setUser_id() {
	this.user_id = user_id;
	}

	public String getSentiment() {
	return sentiment;
	}
	public void setSentiment() {
	this.sentiment = sentiment;
	}

}
