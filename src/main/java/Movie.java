import java.util.List;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "perf_test", name = "movie")
public class Movie {
	@PartitionKey
	@Column(name = "id")
	private int id;
	@Column(name = "name")
	private String name;
	@Column(name = "online_date")
	private LocalDate date;
	@Column(name = "act_list")
	private List<Actor> act;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public LocalDate getDate() {
		return date;
	}

	public void setDate(LocalDate date) {
		this.date = date;
	}

	public List<Actor> getAct() {
		return act;
	}

	public void setAct(List<Actor> act) {
		this.act = act;
	}

	public Movie(int id, String name, LocalDate date, List<Actor> act) {
		super();
		this.id = id;
		this.name = name;
		this.date = date;
		this.act = act;
	}
}