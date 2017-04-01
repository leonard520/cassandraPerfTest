import java.nio.ByteBuffer;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class MovieCodec extends TypeCodec<Movie> {
	private final TypeCodec<UDTValue> innerCodec;

    private final UserType userType;

    public MovieCodec(TypeCodec<UDTValue> innerCodec, Class<Movie> javaType) {
        super(innerCodec.getCqlType(), javaType);
        this.innerCodec = innerCodec;
        this.userType = (UserType)innerCodec.getCqlType();
    }

    @Override
    public ByteBuffer serialize(Movie value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return innerCodec.serialize(toUDTValue(value), protocolVersion);
    }

    @Override
    public Movie deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return toMovie(innerCodec.deserialize(bytes, protocolVersion));
    }

    @Override
    public Movie parse(String value) throws InvalidTypeException {
        return value == null || value.isEmpty() || value.equals(null) ? null : toMovie(innerCodec.parse(value));
    }

    @Override
    public String format(Movie value) throws InvalidTypeException {
        return value == null ? null : innerCodec.format(toUDTValue(value));
    }

    protected Movie toMovie(UDTValue value) {
        return value == null ? null : new Movie(
        	value.getInt("id"),
            value.getString("name"), 
            value.getDate("date"),
            value.getList("act", Actor.class)
        );
    }

    protected UDTValue toUDTValue(Movie value) {
        return value == null ? null : userType.newValue()
        		.setInt("id", value.getId())
        		.setString("name", value.getName())
        		.setDate("date", value.getDate())
        		.setList("act", value.getAct());
    }
}
