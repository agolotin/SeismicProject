package main.java.edu.byu.seismicproject.signalprocessing;

import java.util.ArrayList;
import java.util.Collection;

public enum DetectorType {

    SUBSPACE(               9, "Subspace",             "SUBSPACE", false ),
    CORRELATION(            8, "Correlation",          "CORR",     false ),
    ARRAY_CORRELATION(      7, "ArrayCorrelation",     "ARR_CORR", false ),
    COHERENT_MATCHED_FIELD( 6, "CoherentMatchedField", "CMFD",     true  ),
    MATCHED_FIELD(          5, "MatchedField",         "MATCHFLD", true  ),
    ARRAYPOWER(             4, "ArrayPower",           "ARR_POW",  true  ),
    FSTATISTIC(             3, "Fstatistic",           "FSTAT",    false ),
    BULLETIN(               2, "Bulletin",             "BULLETIN", true  ),
    STALTA(                 1, "STALTA",               "STALTA",   true  );

    private static final int[] ranks = new int[]{ 9, 8, 7, 6, 5, 4, 3, 2, 1 };
    private final String       shortName;
    private final String       name;
    private final int          priority;
    private final boolean      spawning;


    private DetectorType( int priority, String name, String shortName, boolean spawning ) {
        this.priority  = priority;
        this.name      = name;
        this.shortName = shortName;
        this.spawning  = spawning;
    }

    public static int[] getRanks() {
        return ranks;
    }

    public static DetectorType getByPriority(int priority) {
        for (DetectorType type : DetectorType.values()) {
            if (type.getPriority() == priority) {
                return type;
            }
        }
        throw new IllegalArgumentException("Illegal priority value: " + priority);
    }

    public int getPriority() {
        return priority;
    }

    /**
     * @return the shortName
     */
    public String getShortName() {
        return shortName;
    }

    /**
     * @return the spawning
     */
    public boolean isSpawning() {
        return spawning;
    }

    public static Collection<DetectorType> getSpawningDetectorTypes() {
        ArrayList<DetectorType> result = new ArrayList<>();
        for (DetectorType type : DetectorType.values()) {
            if (type.isSpawning()) {
                result.add(type);
            }
        }
        return result;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }
}
