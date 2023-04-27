package utils;

import operators.IKeySelector;

public class KeyUtil {
    public static int getHashFromKey(String key){
        System.out.println("KeyUtil.getHashFromKey: " + key);
        assert key.contains(":keyed:");
        String[] parts = key.split(":keyed:");
        assert parts.length == 2;
        String hashString = parts[1].split(":")[0];
        int hash = Integer.parseInt(hashString.substring(2), 10);
        return hash;
    }

    public static String objToKey(Object o, IKeySelector keySelector){
        String uniqueKey = keySelector.getUniqueKey(o);
        int hashResult = hashStringToInt(uniqueKey);
        String fmt = ":keyed:%s:%s";
        String key = String.format(fmt, intToHex(hashResult), uniqueKey);
        //System.out.println("KeyUtil.objToKey: " + key);
        return key;
    }

    public static int hashStringToInt(String hashString){
        return hashString.hashCode();
    }

    private static String intToHex(int i){
        //int desiredLength = 10;
        //String hexString = Integer.toHexString(i);
        //hexString = String.format("%1$" + (desiredLength - 2) + "s", hexString).replace(' ', '0');
        //hexString = "0x" + hexString;
        //assert hexString.length() == 10;

        return "HC"+i;
    }

}
