package com.lordofthejars.nosqlunit.neo4j.extension.springtemplate.integration;

import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.springframework.data.neo4j.annotation.Fetch;
import org.springframework.data.neo4j.annotation.GraphId;
import org.springframework.data.neo4j.annotation.Indexed;
import org.springframework.data.neo4j.annotation.NodeEntity;
import org.springframework.data.neo4j.annotation.RelatedTo;

@NodeEntity
public class Person {

	@GraphId Long nodeId;
	
	@Indexed
	private String name;

	@Fetch @RelatedTo(direction = Direction.BOTH, elementClass = Person.class)
	private Set<Person> friends;

	public Person() {
	}

	public Person(String name) {
		this.name = name;
	}
	
	public Set<Person> getFriends() {
		return friends;
	}
	
	public void setFriends(Set<Person> friends) {
		this.friends = friends;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	private void knows(Person friend) {
		friends.add(friend);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		Person other = (Person) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Person [id="+nodeId+" name=" + name + "]";
	}

}
