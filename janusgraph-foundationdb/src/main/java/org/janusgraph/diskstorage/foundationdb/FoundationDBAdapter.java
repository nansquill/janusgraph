package org.janusgraph.diskstorage.foundationdb;

import com.apple.foundationdb.*;
import com.apple.foundationdb.tuple.Tuple;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class FoundationDBAdapter {
    private static final FDB fdb;
    private static final Database db;

    static {
        fdb = FDB.selectAPIVersion(630);
        db = fdb.open();
        db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
        db.options().setTransactionRetryLimit(100);
    }

    // Generate 1,620 classes like '9:00 chem for dummies'
    private static List<String> levels = Arrays.asList("intro", "for dummies",
        "remedial", "101", "201", "301", "mastery", "lab", "seminar");

    private static List<String> types = Arrays.asList("chem", "bio", "cs",
        "geometry", "calc", "alg", "film", "music", "art", "dance");

    private static List<String> times = Arrays.asList("2:00", "3:00", "4:00",
        "5:00", "6:00", "7:00", "8:00", "9:00", "10:00", "11:00", "12:00", "13:00",
        "14:00", "15:00", "16:00", "17:00", "18:00", "19:00");

    private static List<String> classNames = initClassNames();

    private static List<String> initClassNames() {
        List<String> classNames = new ArrayList<String>();
        for (String level: levels)
            for (String type: types)
                for (String time: times)
                    classNames.add(time + " " + type + " " + level);
        return classNames;
    }

    private static void addClass(TransactionContext db, final String c) {
        db.run((Transaction tr) -> {
            tr.set(Tuple.from("class", c).pack(), encodeInt(100));
            return null;
        });
    }

    private static byte[] encodeInt(int value) {
        byte[] output = new byte[4];
        ByteBuffer.wrap(output).putInt(value);
        return output;
    }

    private static int decodeInt(byte[] value) {
        if (value.length != 4)
            throw new IllegalArgumentException("Array must be of size 4");
        return ByteBuffer.wrap(value).getInt();
    }

    private static void init(Database db) {
        db.run((Transaction tr) -> {
            tr.clear(Tuple.from("attends").range());
            tr.clear(Tuple.from("class").range());
            for (String className: classNames)
                addClass(tr, className);
            return null;
        });
    }

    private static List<String> availableClasses(TransactionContext db) {
        return db.run((Transaction tr) -> {
            List<String> classNames = new ArrayList<String>();
            for(KeyValue kv: tr.getRange(Tuple.from("class").range())) {
                if (decodeInt(kv.getValue()) > 0)
                    classNames.add(Tuple.fromBytes(kv.getKey()).getString(1));
            }
            return classNames;
        });
    }

    private static void drop(TransactionContext db, final String s, final String c) {
        db.run((Transaction tr) -> {
            byte[] rec = Tuple.from("attends", s, c).pack();
            if (tr.get(rec).join() == null)
                return null; // not taking this class
            byte[] classKey = Tuple.from("class", c).pack();
            tr.set(classKey, encodeInt(decodeInt(tr.get(classKey).join()) + 1));
            tr.clear(rec);
            return null;
        });
    }

    private static void signup(TransactionContext db, final String s, final String c) {
        db.run((Transaction tr) -> {
            byte[] rec = Tuple.from("attends", s, c).pack();
            if (tr.get(rec).join() != null)
                return null; // already signed up

            int seatsLeft = decodeInt(tr.get(Tuple.from("class", c).pack()).join());
            if (seatsLeft == 0)
                throw new IllegalStateException("No remaining seats");

            List<KeyValue> classes = tr.getRange(Tuple.from("attends", s).range()).asList().join();
            if (classes.size() == 5)
                throw new IllegalStateException("Too many classes");

            tr.set(Tuple.from("class", c).pack(), encodeInt(seatsLeft - 1));
            tr.set(rec, Tuple.from("").pack());
            return null;
        });
    }

    private static void switchClasses(TransactionContext db, final String s, final String oldC, final String newC) {
        db.run((Transaction tr) -> {
            drop(tr, s, oldC);
            signup(tr, s, newC);
            return null;
        });
    }

    //
    // Testing
    //

    private static void simulateStudents(int i, int ops) {

        String studentID = "s" + Integer.toString(i);
        List<String> allClasses = classNames;
        List<String> myClasses = new ArrayList<String>();

        String c;
        String oldC;
        String newC;
        Random rand = new Random();

        for (int j=0; j<ops; j++) {
            int classCount = myClasses.size();
            List<String> moods = new ArrayList<String>();
            if (classCount > 0) {
                moods.add("drop");
                moods.add("switch");
            }
            if (classCount < 5)
                moods.add("add");
            String mood = moods.get(rand.nextInt(moods.size()));

            try {
                if (allClasses.isEmpty())
                    allClasses = availableClasses(db);
                if (mood.equals("add")) {
                    c = allClasses.get(rand.nextInt(allClasses.size()));
                    signup(db, studentID, c);
                    myClasses.add(c);
                } else if (mood.equals("drop")) {
                    c = myClasses.get(rand.nextInt(myClasses.size()));
                    drop(db, studentID, c);
                    myClasses.remove(c);
                } else if (mood.equals("switch")) {
                    oldC = myClasses.get(rand.nextInt(myClasses.size()));
                    newC = allClasses.get(rand.nextInt(allClasses.size()));
                    switchClasses(db, studentID, oldC, newC);
                    myClasses.remove(oldC);
                    myClasses.add(newC);
                }
            } catch (Exception e) {
                System.out.println(e.getMessage() +  "Need to recheck available classes.");
                allClasses.clear();
            }

        }

    }

    private static void runSim(int students, final int ops_per_student) throws InterruptedException {
        List<Thread> threads = new ArrayList<Thread>(students);//Thread[students];
        for (int i = 0; i < students; i++) {
            final int j = i;
            threads.add(new Thread(() -> simulateStudents(j, ops_per_student)) );
        }
        for (Thread thread: threads)
            thread.start();
        for (Thread thread: threads)
            thread.join();
        System.out.format("Ran %d transactions%n", students * ops_per_student);
    }

    public static void main(String[] args) throws InterruptedException {
        init(db);
        System.out.println("Initialized");
        runSim(10,10);
    }

}
