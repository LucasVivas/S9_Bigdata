import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaggedValue implements Writable{

    private String name;
    private boolean isCity;

    public TaggedValue() {
        name = "";
        isCity = true;
    }

    public TaggedValue(String name, boolean isCity) {
        this.name = name;
        this.isCity = isCity;
    }

    public String getName() {
        return name;
    }

    public boolean isCity() {
        return isCity;
    }

    @Override
    public String toString() {
        return "{name: "+name+", isCity: "+ (isCity ? "True":"False") +"}";
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeBoolean(isCity);
    }

    public void readFields(DataInput dataInput) throws IOException {
        name = dataInput.readUTF();
        isCity = dataInput.readBoolean();
    }

    public TaggedValue clone(){
        return new TaggedValue(name, isCity);
    }
}
