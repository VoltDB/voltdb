package tracker;

public class Satellite {
    public int id;
    public String model;
    public String country;
    public double currentLat;
    public double currentLong;

    public Satellite(int identifier, String model_number, String owner)  {
        id = identifier;
        model = model_number;
        country = owner;
    }
}
