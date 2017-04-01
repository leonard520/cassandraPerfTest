import java.nio.ByteBuffer;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class ActorCodec extends TypeCodec<Actor> {
	private final TypeCodec<UDTValue> innerCodec;

    private final UserType userType;

    public ActorCodec(TypeCodec<UDTValue> innerCodec, Class<Actor> javaType) {
        super(innerCodec.getCqlType(), javaType);
        this.innerCodec = innerCodec;
        this.userType = (UserType)innerCodec.getCqlType();
    }

    @Override
    public ByteBuffer serialize(Actor value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return innerCodec.serialize(toUDTValue(value), protocolVersion);
    }

    @Override
    public Actor deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return toActor(innerCodec.deserialize(bytes, protocolVersion));
    }

    @Override
    public Actor parse(String value) throws InvalidTypeException {
        return value == null || value.isEmpty() || value.equals(null) ? null : toActor(innerCodec.parse(value));
    }

    @Override
    public String format(Actor value) throws InvalidTypeException {
        return value == null ? null : innerCodec.format(toUDTValue(value));
    }

    protected Actor toActor(UDTValue value) {
        return value == null ? null : new Actor(
        	value.getInt("id"),
            value.getString("name"), 
            value.getInt("age")
        );
    }

    protected UDTValue toUDTValue(Actor value) {
        return value == null ? null : userType.newValue()
        		.setInt("id", value.getId())
        		.setString("name", value.getName())
        		.setInt("age", value.getAge());
    }
}
