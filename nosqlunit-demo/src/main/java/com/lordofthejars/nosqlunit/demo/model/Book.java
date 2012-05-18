package com.lordofthejars.nosqlunit.demo.model;


public class Book {

	
	private String title;
	
	private int numberOfPages;
	
	public Book(String title, int numberOfPages) {
		super();
		this.title = title;
		this.numberOfPages = numberOfPages;
	}
	
	public void setTitle(String title) {
		this.title = title;
	}
	
	public void setNumberOfPages(int numberOfPages) {
		this.numberOfPages = numberOfPages;
	}
	
	
	public String getTitle() {
		return title;
	}
	
	public int getNumberOfPages() {
		return numberOfPages;
	}

	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + numberOfPages;
		result = prime * result + ((title == null) ? 0 : title.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Book other = (Book) obj;
		if (numberOfPages != other.numberOfPages)
			return false;
		if (title == null) {
			if (other.title != null)
				return false;
		} else if (!title.equals(other.title))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Book [title=").append(title)
				.append(", numberOfPages=").append(numberOfPages).append("]");
		return builder.toString();
	}
	
	
}
