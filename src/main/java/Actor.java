import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

@UDT(keyspace = "perf_test", name = "act_info")
public class Actor {
	public Actor(int id, String name, int age) {
		this.id = id;
		this.name = name;
		this.age = age;
	}

	public Actor() {
	}

	@Field(name = "id")
	private int id;
	
	@Field(name = "name")
	private String name;
	
	@Field(name = "age")
	private int age;

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

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	@Override
	public String toString() {
		return "MovieCassandra [id=" + id + ", name=" + name + ", age=" + age + "]";
	}
}