import org.apache.hadoop.io.Writable;

import java.io.*;

public class City implements Comparable, Writable{
    private int pop;
    private String name;

    public City(){
        pop = 0;
        name = "";
    }

    public City(int pop, String name) {
        this.pop = pop;
        this.name = name;
    }


    public void setName(String name) {
        this.name = name;
    }

    public void setPop(int pop) {
        this.pop = pop;
    }

    @Override
    public String toString() {
        return "{name: "+name+", pop: "+pop+"}";
    }

    public int getPop() {
        return pop;
    }

    public String getName() {
        return name;
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof City)){
            throw new RuntimeException();
        }
        City city = (City)o;
        if (pop < city.getPop())
            return -1;
        else if (pop > city.getPop())
            return 1;
        else
            return name.compareTo(city.getName());
    }

/*    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(pop);
        WritableUtils.writeString(dataOutput, name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pop = dataInput.readInt();
        name = WritableUtils.readString(dataInput);
    }
*/
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(pop);
        dataOutput.writeUTF(name);
    }

    public void readFields(DataInput dataInput) throws IOException {
        pop = dataInput.readInt();
        name = dataInput.readUTF();
    }

    public City clone(){
        return new City(pop, name);
    }

}
