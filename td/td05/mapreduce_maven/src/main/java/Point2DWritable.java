import org.apache.hadoop.io.Writable;

import java.awt.geom.Point2D;
import java.io.*;

public class Point2DWritable implements Writable {

    private Point2D.Double point;

    public Point2DWritable() {
        super();
        point = new Point2D.Double();
    }

    public Point2DWritable(double x, double y){
        super();
        point = new Point2D.Double(x,y);
    }

    public void setX(double x) {
        point.setLocation(x,point.getY());
    }

    public void setY(double y) {
        point.setLocation(point.getX(),y);
    }

    public double getX() {
        return point.getX();
    }

    public double getY() {
        return point.getY();
    }

    public void write(DataOutput dataOutput) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        out = new ObjectOutputStream(bos);
        out.writeObject(point);
        out.flush();

        dataOutput.writeInt(bos.size());
        dataOutput.write(bos.toByteArray());
    }

    public void readFields(DataInput dataInput) throws IOException {
        byte[] ba = new byte[dataInput.readInt()];
        dataInput.readFully(ba);
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(ba);
            ObjectInput in = new ObjectInputStream(bis);
            point = (Point2D.Double) in.readObject();
        } catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }
}